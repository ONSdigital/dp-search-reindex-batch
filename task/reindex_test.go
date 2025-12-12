package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	dpEsClient "github.com/ONSdigital/dp-elasticsearch/v3/client"
	v710 "github.com/ONSdigital/dp-elasticsearch/v3/client/elasticsearch/v710"
	mocks "github.com/ONSdigital/dp-search-api/clients/mock"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testCollectionID = "testCollectionID"
	testDatasetID    = "TS056"
	testEdition      = "2021"
	testVersion      = "4"
	testIndexName    = "ons"
	testAuthToken    = "testAuthToken"
)

var (
	ctx     = context.Background()
	testURI = fmt.Sprintf("/datasets/%s/editions/%s/versions/%s", testDatasetID, testEdition, testVersion)
)

func TestCreateIndexName(t *testing.T) {
	Convey("CreateIndexName returns the an index name with the expected prefix", t, func() {
		s0 := createIndexName(testIndexName)
		So(s0, ShouldStartWith, testIndexName)

		Convey("And calling createIndexName again results in a different name", func() {
			s1 := createIndexName(testIndexName)
			So(s1, ShouldNotEqual, s0)
		})
	})
}

func TestIndexDoc(t *testing.T) {
	Convey("Given a successful elasticsearch client mock, a Document channel and an indexed bool channel", t, func() {
		esClient := &mocks.ElasticSearchMock{
			BulkIndexAddFunc: func(
				ctx context.Context,
				action dpEsClient.BulkIndexerAction,
				index string,
				documentID string,
				document []byte,
				onSuccess dpEsClient.SuccessFunc,
				onFailure dpEsClient.FailureFunc,
			) error {
				onSuccess(ctx, esutil.BulkIndexerItem{}, esutil.BulkIndexerResponseItem{})
				return nil
			},
		}
		transformedChan := make(chan Document, 1)
		indexedChan := make(chan bool)
		tracker := &Tracker{}

		Convey("When a Document is sent to the transformedChan and consumed by indexDoc", func() {
			transformedChan <- Document{
				ID:   testDatasetID,
				URI:  testURI,
				Body: []byte{1, 2, 3, 4},
			}
			close(transformedChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				indexDoc(ctx, tracker, esClient, transformedChan, indexedChan, testIndexName)
			}()

			Convey("Then the document is indexed and the expected bulkAdd call is performed", func() {
				indexed := <-indexedChan
				So(indexed, ShouldBeTrue)

				So(esClient.BulkIndexAddCalls(), ShouldHaveLength, 1)
				So(esClient.BulkIndexAddCalls()[0].Action, ShouldEqual, v710.Create)
				So(esClient.BulkIndexAddCalls()[0].Document, ShouldResemble, []byte{1, 2, 3, 4})
				So(esClient.BulkIndexAddCalls()[0].DocumentID, ShouldEqual, testDatasetID)
				So(esClient.BulkIndexAddCalls()[0].Index, ShouldEqual, testIndexName)

				wg.Wait()
			})
		})
	})

	Convey("Given an elasticsearch client mock that returns an error, a Document channel and an indexed bool channel", t, func() {
		esClient := &mocks.ElasticSearchMock{
			BulkIndexAddFunc: func(
				ctx context.Context,
				action dpEsClient.BulkIndexerAction,
				index string,
				documentID string,
				document []byte,
				onSuccess dpEsClient.SuccessFunc,
				onFailure dpEsClient.FailureFunc,
			) error {
				return errors.New("testError")
			},
		}
		transformedChan := make(chan Document, 1)
		indexedChan := make(chan bool)
		tracker := &Tracker{}

		Convey("When a Document is sent to the transformedChan and consumed by indexDoc", func() {
			transformedChan <- Document{
				ID:   testDatasetID,
				URI:  testURI,
				Body: []byte{1, 2, 3, 4},
			}
			close(transformedChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				indexDoc(ctx, tracker, esClient, transformedChan, indexedChan, testIndexName)
			}()

			Convey("Then the document is not indexed and the expected bulkAdd call is performed", func() {
				indexed := <-indexedChan
				So(indexed, ShouldBeFalse)

				So(esClient.BulkIndexAddCalls(), ShouldHaveLength, 1)
				So(esClient.BulkIndexAddCalls()[0].Action, ShouldEqual, v710.Create)
				So(esClient.BulkIndexAddCalls()[0].Document, ShouldResemble, []byte{1, 2, 3, 4})
				So(esClient.BulkIndexAddCalls()[0].DocumentID, ShouldEqual, testDatasetID)
				So(esClient.BulkIndexAddCalls()[0].Index, ShouldEqual, testIndexName)

				wg.Wait()
			})
		})
	})

	Convey("Given an elasticsearch client mock that fails asynchronously, a Document channel and an indexed bool channel", t, func() {
		esClient := &mocks.ElasticSearchMock{
			BulkIndexAddFunc: func(
				ctx context.Context,
				action dpEsClient.BulkIndexerAction,
				index string,
				documentID string,
				document []byte,
				onSuccess dpEsClient.SuccessFunc,
				onFailure dpEsClient.FailureFunc,
			) error {
				onFailure(ctx, esutil.BulkIndexerItem{}, esutil.BulkIndexerResponseItem{}, errors.New("testError"))
				return nil
			},
		}
		transformedChan := make(chan Document, 1)
		indexedChan := make(chan bool)
		tracker := &Tracker{}

		Convey("When a Document is sent to the transformedChan and consumed by indexDoc", func() {
			transformedChan <- Document{
				ID:   testDatasetID,
				URI:  testURI,
				Body: []byte{1, 2, 3, 4},
			}
			close(transformedChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				indexDoc(ctx, tracker, esClient, transformedChan, indexedChan, testIndexName)
			}()

			Convey("Then the document is not indexed and the expected bulkAdd call is performed", func() {
				indexed := <-indexedChan
				So(indexed, ShouldBeFalse)

				So(esClient.BulkIndexAddCalls(), ShouldHaveLength, 1)
				So(esClient.BulkIndexAddCalls()[0].Action, ShouldEqual, v710.Create)
				So(esClient.BulkIndexAddCalls()[0].Document, ShouldResemble, []byte{1, 2, 3, 4})
				So(esClient.BulkIndexAddCalls()[0].DocumentID, ShouldEqual, testDatasetID)
				So(esClient.BulkIndexAddCalls()[0].Index, ShouldEqual, testIndexName)

				wg.Wait()
			})
		})
	})
}
