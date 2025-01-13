package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	upstreamModels "github.com/ONSdigital/dis-search-upstream-stub/models"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/zebedee"
	dpEsClient "github.com/ONSdigital/dp-elasticsearch/v3/client"
	v710 "github.com/ONSdigital/dp-elasticsearch/v3/client/elasticsearch/v710"
	mocks "github.com/ONSdigital/dp-search-api/clients/mock"
	importerModels "github.com/ONSdigital/dp-search-data-importer/models"
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

func TestURIProducer(t *testing.T) {
	Convey("Given a zebedeeClient that returns results", t, func() {
		tracker := &Tracker{}
		errChan := make(chan error, 1)

		zebClient := &mocks.ZebedeeClientMock{
			GetPublishedIndexFunc: func(ctx context.Context, piRequest *zebedee.PublishedIndexRequestParams) (zebedee.PublishedIndex, error) {
				return zebedee.PublishedIndex{
					Count: 4,
					Items: []zebedee.PublishedIndexItem{
						{URI: "/economy"},
						{URI: "/timeseries/ec12/previous/nov2011"},
						{URI: "/timeseries/ec12"},
						{URI: "/dataset/ec12/previous"},
					},
					Limit:      20,
					Offset:     0,
					TotalCount: 4,
				}, nil
			},
		}

		Convey("When the URI Producer is called", func() {
			uriChan := uriProducer(ctx, tracker, errChan, zebClient)

			Convey("Then the expected uris are sent to the uris channel", func() {
				uris := make([]string, 3)
				for uri := range uriChan {
					fmt.Println(uri)
					uris = append(uris, uri)
				}

				So(uris, ShouldContain, "/economy")
				So(uris, ShouldContain, "/timeseries/ec12")
				So(uris, ShouldContain, "/dataset/ec12/previous")
				So(uris, ShouldNotContain, "/timeseries/ec12/previous/nov2011")
			})
		})
	})
}

func TestTransformMetadataDoc(t *testing.T) {
	Convey("Given a metadata channel and a transformed document channel", t, func() {
		metadataChan := make(chan *dataset.Metadata, 1)
		transformedChan := make(chan Document, 1)
		tracker := &Tracker{}

		Convey("When a generic dataset metadata is sent to the channel and consumed by transformMetadataDoc", func() {
			sent := &dataset.Metadata{
				DatasetLinks: dataset.Links{
					LatestVersion: dataset.Link{
						URL: fmt.Sprintf("http://testHost:123%s", testURI),
					},
				},
				DatasetDetails: dataset.DatasetDetails{
					ID: testDatasetID,
					IsBasedOn: &dataset.IsBasedOn{
						Type: "testType",
						ID:   "testID",
					},
				},
			}

			expected := &importerModels.EsModel{
				DataType:       "dataset_landing_page",
				URI:            testURI,
				DatasetID:      testDatasetID,
				Edition:        testEdition,
				PopulationType: &importerModels.EsPopulationType{},
			}

			metadataChan <- sent
			close(metadataChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func(waitGroup *sync.WaitGroup) {
				transformMetadataDoc(ctx, tracker, nil, metadataChan, transformedChan)
				wg.Done()
			}(wg)

			Convey("Then the expected elasticsearch document is sent to the transformed channel", func() {
				transformed := <-transformedChan
				So(transformed.ID, ShouldEqual, testDatasetID)
				So(transformed.URI, ShouldEqual, testURI)

				esModel := &importerModels.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				So(err, ShouldBeNil)
				So(esModel, ShouldResemble, expected)

				wg.Wait()
			})
		})
	})

	Convey("Given a metadata channel and a transformed document channel", t, func() {
		metadataChan := make(chan *dataset.Metadata, 1)
		transformedChan := make(chan Document, 1)
		tracker := &Tracker{}

		Convey("When a cantabular type dataset metadata is sent to the channel and consumed by transformMetadataDoc", func() {
			areaTypeTrue := true
			areaTypeFalse := false
			sent := &dataset.Metadata{
				DatasetLinks: dataset.Links{
					LatestVersion: dataset.Link{
						URL: fmt.Sprintf("http://testHost:123%s", testURI),
					},
				},
				DatasetDetails: dataset.DatasetDetails{
					ID: testDatasetID,
					IsBasedOn: &dataset.IsBasedOn{
						ID:   "UR_HH",
						Type: "cantabular_flexible_table",
					},
				},
				Version: dataset.Version{
					Dimensions: []dataset.VersionDimension{
						{ID: "dim1", Label: "label 1 (10 categories)"},
						{ID: "dim2", Label: "label 2 (12 Categories)", IsAreaType: &areaTypeFalse},
						{ID: "dim3", IsAreaType: &areaTypeTrue},
						{ID: "dim4", Label: "label 4 (1 category)"},
					},
				},
			}

			expected := &importerModels.EsModel{
				DataType:  "dataset_landing_page", // dataset_landing_page type is used for cantabular types
				URI:       testURI,
				DatasetID: testDatasetID,
				Edition:   testEdition,
				PopulationType: &importerModels.EsPopulationType{
					Key:    "all-usual-residents-in-households",
					AggKey: "all-usual-residents-in-households###All usual residents in households",
					Name:   "UR_HH",
					Label:  "All usual residents in households",
				},
				Dimensions: []importerModels.EsDimension{
					{Key: "label-1", AggKey: "label-1###label 1", Name: "dim1", RawLabel: "label 1 (10 categories)", Label: "label 1"},
					{Key: "label-2", AggKey: "label-2###label 2", Name: "dim2", RawLabel: "label 2 (12 Categories)", Label: "label 2"},
					{Key: "label-4", AggKey: "label-4###label 4", Name: "dim4", RawLabel: "label 4 (1 category)", Label: "label 4"},
				},
			}

			metadataChan <- sent
			close(metadataChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func(waitGroup *sync.WaitGroup) {
				transformMetadataDoc(ctx, tracker, nil, metadataChan, transformedChan)
				wg.Done()
			}(wg)

			Convey("Then the expected elasticsearch document is sent to the transformed channel", func() {
				transformed := <-transformedChan
				So(transformed.ID, ShouldEqual, testDatasetID)
				So(transformed.URI, ShouldEqual, testURI)

				esModel := &importerModels.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				So(err, ShouldBeNil)
				So(esModel.DataType, ShouldEqual, expected.DataType)
				So(esModel.URI, ShouldEqual, expected.URI)
				So(esModel.DatasetID, ShouldEqual, expected.DatasetID)
				So(esModel.Edition, ShouldEqual, expected.Edition)
				So(esModel.PopulationType, ShouldResemble, expected.PopulationType)
				So(esModel.Dimensions, ShouldHaveLength, len(expected.Dimensions))
				for _, dim := range expected.Dimensions {
					So(esModel.Dimensions, ShouldContain, dim)
				}

				wg.Wait()
			})
		})
	})
}

func TestTransformResourceItem(t *testing.T) {
	Convey("Given a resource channel and a transformed document channel and a topics map", t, func() {
		resourceChan := make(chan upstreamModels.Resource, 1)
		transformedResChan := make(chan Document, 1)
		tracker := &Tracker{}
		errChan := make(chan error, 1)

		topicsMap := make(map[string]Topic)
		topicsMap["a"] = Topic{ID: "id",
			Slug:       "economy",
			ParentID:   "",
			ParentSlug: ""}

		Convey("When a release resource item is sent to the channel and consumed by transformResourceItem", func() {
			sent := upstreamModels.Resource{
				URI:             "/a/uri",
				URIOld:          "/an/old/uri",
				ContentType:     "release",
				CDID:            "A321B",
				DatasetID:       "ASELECTIONOFNUMBERSANDLETTERS456",
				Edition:         "an edition",
				MetaDescription: "a description",
				ReleaseDate:     "2024-11-21:20:14Z",
				Summary:         "a summary",
				Title:           "a title",
				Language:        "string",
				Survey:          "string",
				CanonicalTopic:  "string",
				Cancelled:       true,
				Finalised:       true,
				Published:       true,
				ProvisionalDate: "October-November 2024",
				DateChanges: []upstreamModels.ReleaseDateDetails{
					{
						ChangeNotice: "a change_notice",
						PreviousDate: "2023-11-21:20:14Z",
					},
				},
			}

			expected := &importerModels.EsModel{
				URI:             "/a/uri",
				Edition:         "an edition",
				DataType:        "release",
				CDID:            "A321B",
				DatasetID:       "ASELECTIONOFNUMBERSANDLETTERS456",
				MetaDescription: "a description",
				ReleaseDate:     "2024-11-21:20:14Z",
				Summary:         "a summary",
				Title:           "a title",
				Topics:          []string{},
				Language:        "string",
				Survey:          "string",
				CanonicalTopic:  "string",
				Cancelled:       true,
				Finalised:       true,
				Published:       true,
				ProvisionalDate: "October-November 2024",
				DateChanges: []importerModels.ReleaseDateChange{
					{
						ChangeNotice: "a change_notice",
						Date:         "2023-11-21:20:14Z",
					},
				},
				PopulationType: &importerModels.EsPopulationType{Key: "", AggKey: "", Name: "", Label: ""},
				Dimensions:     []importerModels.EsDimension(nil),
			}

			resourceChan <- sent
			close(resourceChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func(waitGroup *sync.WaitGroup) {
				transformResourceItem(ctx, tracker, errChan, resourceChan, transformedResChan, topicsMap)
				wg.Done()
			}(wg)

			Convey("Then the expected elasticsearch document is sent to the transformed channel", func() {
				transformed := <-transformedResChan
				So(transformed.ID, ShouldEqual, "/a/uri")
				So(transformed.URI, ShouldEqual, "/a/uri")

				esModel := &importerModels.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				So(err, ShouldBeNil)
				So(esModel, ShouldResemble, expected)

				wg.Wait()
			})
		})
	})
}

func TestExtractDatasets(t *testing.T) {
	Convey("Given a dataset client that succeeds to return up to 3 datasets", t, func() {
		items := []dataset.Dataset{
			{ID: "dataset1"},
			{ID: "dataset2"},
			{ID: "dataset3"},
		}
		cli := &mocks.DatasetAPIClientMock{
			GetDatasetsFunc: func(ctx context.Context, userAuthToken, serviceAuthToken, collectionID string, q *dataset.QueryParams) (dataset.List, error) {
				getItems := func() []dataset.Dataset {
					if q.Offset >= len(items) {
						return []dataset.Dataset{}
					}
					if q.Offset+q.Limit < len(items) {
						return items[q.Offset : q.Offset+q.Limit]
					}
					return items[q.Offset:]
				}

				it := getItems()
				return dataset.List{
					Items:      it,
					Offset:     q.Offset,
					Limit:      q.Limit,
					Count:      len(it),
					TotalCount: len(items),
				}, nil
			},
		}
		tracker := &Tracker{}

		Convey("Then extractDatasets with a paginationLimit of 2 send all the datasets to the dataset channel", func() {
			datasetChan, wg := extractDatasets(ctx, tracker, nil, cli, testAuthToken, 2)

			ds1 := <-datasetChan
			So(ds1, ShouldResemble, dataset.Dataset{ID: "dataset1"})
			ds2 := <-datasetChan
			So(ds2, ShouldResemble, dataset.Dataset{ID: "dataset2"})
			ds3 := <-datasetChan
			So(ds3, ShouldResemble, dataset.Dataset{ID: "dataset3"})
			wg.Wait()

			Convey("And dataset api has been called twice with the expected pagination parameters", func() {
				So(cli.GetDatasetsCalls(), ShouldHaveLength, 2)
				So(cli.GetDatasetsCalls()[0].Q.Offset, ShouldEqual, 0)
				So(cli.GetDatasetsCalls()[0].Q.Limit, ShouldEqual, 2)
				So(cli.GetDatasetsCalls()[0].ServiceAuthToken, ShouldEqual, testAuthToken)
				So(cli.GetDatasetsCalls()[1].Q.Offset, ShouldEqual, 2)
				So(cli.GetDatasetsCalls()[1].Q.Limit, ShouldEqual, 2)
				So(cli.GetDatasetsCalls()[1].ServiceAuthToken, ShouldEqual, testAuthToken)
			})
		})
	})
}

func TestRetrieveDatasetEditions(t *testing.T) {
	testEditionDetails := []dataset.EditionsDetails{
		{
			ID: "editionZero",
			Current: dataset.Edition{
				Edition: testEdition,
				Links: dataset.Links{
					LatestVersion: dataset.Link{
						ID: testVersion,
					},
				},
			},
		},
		{
			Current: dataset.Edition{
				Edition: "shouldBeIgnored",
			},
		},
	}
	testIsBasedOn := dataset.IsBasedOn{
		ID:   "UR_HH",
		Type: "cantabular_flexible_table",
	}

	Convey("Given a dataset client that succeeds to return multiple editions where only one has ID and link, and a datasetChan channel", t, func() {
		cli := &mocks.DatasetAPIClientMock{
			GetFullEditionsDetailsFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, datasetID string) ([]dataset.EditionsDetails, error) {
				return testEditionDetails, nil
			},
		}
		datasetChan := make(chan dataset.Dataset, 1)
		tracker := &Tracker{}

		Convey("When a valid dataset is sent to the dataset channel and consumed by retrieveDatasetEditions", func() {
			datasetChan <- dataset.Dataset{
				Current: &dataset.DatasetDetails{
					ID:        testDatasetID,
					IsBasedOn: &testIsBasedOn,
				},
				DatasetDetails: dataset.DatasetDetails{
					CollectionID: testCollectionID,
				},
			}
			close(datasetChan)

			editionChan, _ := retrieveDatasetEditions(ctx, tracker, cli, datasetChan, testAuthToken, 10)

			Convey("Then the expected editions and isBasedOn are sent to the edition channel returned by retrieveDatasetEditions", func() {
				ed1 := <-editionChan
				So(ed1, ShouldResemble, DatasetEditionMetadata{
					id:        testDatasetID,
					editionID: testEdition,
					version:   testVersion,
				})

				Convey("And the expected call is performed against dataset api", func() {
					So(cli.GetFullEditionsDetailsCalls(), ShouldHaveLength, 1)
					So(cli.GetFullEditionsDetailsCalls()[0].DatasetID, ShouldEqual, testDatasetID)
					So(cli.GetFullEditionsDetailsCalls()[0].CollectionID, ShouldEqual, testCollectionID)
					So(cli.GetFullEditionsDetailsCalls()[0].ServiceAuthToken, ShouldEqual, testAuthToken)
				})
			})
		})
	})
}

func TestRetrieveLatestMetadata(t *testing.T) {
	testMetadata := dataset.Metadata{
		DatasetLinks: dataset.Links{
			LatestVersion: dataset.Link{
				URL: "latestURL",
			},
		},
		DatasetDetails: dataset.DatasetDetails{
			IsBasedOn: &dataset.IsBasedOn{
				ID:   "UR_HH",
				Type: "cantabular_flexible_table",
			},
		},
	}

	Convey("Given a dataset client that succeeds to return valid metadata and an editionMetadata channel", t, func() {
		cli := &mocks.DatasetAPIClientMock{
			GetVersionMetadataFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, id string, edition string, version string) (dataset.Metadata, error) {
				return testMetadata, nil
			},
		}
		editionMetadata := make(chan DatasetEditionMetadata, 1)
		tracker := &Tracker{}

		Convey("When a dataset edition metadata is sent to the edition metadata channel and consumed by retrieveLatestMetadata", func() {
			editionMetadata <- DatasetEditionMetadata{
				id:        testDatasetID,
				editionID: testEdition,
				version:   testVersion,
			}
			close(editionMetadata)

			metadataChan, _ := retrieveLatestMetadata(ctx, tracker, cli, editionMetadata, testAuthToken, 10)

			Convey("Then the expected metadata and isBasedOn are sent to the metadataChannel", func() {
				m := <-metadataChan
				So(m, ShouldResemble, &testMetadata)

				Convey("And the expected call is performed against dataset api", func() {
					So(cli.GetVersionMetadataCalls(), ShouldHaveLength, 1)
					So(cli.GetVersionMetadataCalls()[0].ID, ShouldEqual, testDatasetID)
					So(cli.GetVersionMetadataCalls()[0].Edition, ShouldEqual, testEdition)
					So(cli.GetVersionMetadataCalls()[0].Version, ShouldEqual, testVersion)
					So(cli.GetVersionMetadataCalls()[0].ServiceAuthToken, ShouldEqual, testAuthToken)
				})
			})
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

func TestTagImportDataTopics(t *testing.T) {
	Convey("Given a valid SearchDataImport while transforming zebedeeDoc ", t, func() {
		topicsMap := map[string]Topic{
			"economy":               {ID: "6734", Slug: "economy", ParentSlug: ""},
			"environmentalaccounts": {ID: "1834", Slug: "environmentalaccounts", ParentSlug: "economy"},
			"business":              {ID: "1234", Slug: "business", ParentSlug: ""},
		}

		importerEventData := importerModels.SearchDataImport{
			UID:             "12345",
			URI:             "/economy/environmentalaccounts",
			DataType:        "dataset",
			Edition:         "2021",
			JobID:           "job-123",
			SearchIndex:     "search-index",
			CanonicalTopic:  "canonical-topic",
			CDID:            "CDID-123",
			DatasetID:       "dataset-123",
			Keywords:        []string{"keyword1", "keyword2"},
			MetaDescription: "meta description",
			ReleaseDate:     "2021-01-01",
			Summary:         "summary",
			Title:           "title",
			Topics:          []string{"1234"},
			TraceID:         "trace-123",
			Cancelled:       false,
			Finalised:       false,
			ProvisionalDate: "2021-01-02",
			Published:       true,
			Survey:          "survey",
			Language:        "en",
		}
		Convey("When topics are initially empty", func() {
			importerEventData.Topics = []string{}
			updatedImporterEventData := tagImportDataTopics(topicsMap, importerEventData)

			Convey("Then the resulting topics should contain only topics from the URI", func() {
				expectedTopics := []string{"6734", "1834"}

				So(updatedImporterEventData.Topics, ShouldHaveLength, len(expectedTopics))
				So(updatedImporterEventData.Topics, ShouldContain, "1834")
				So(updatedImporterEventData.Topics, ShouldContain, "6734")
			})
		})

		Convey("When topics are initially not empty", func() {
			updatedImporterEventData := tagImportDataTopics(topicsMap, importerEventData)

			Convey("Then the resulting topics should contain both the default and the topics obtained from the URI", func() {
				expectedTopics := []string{"1234", "6734", "1834"}

				So(updatedImporterEventData.Topics, ShouldHaveLength, len(expectedTopics))
				So(updatedImporterEventData.Topics, ShouldContain, "1834")
				So(updatedImporterEventData.Topics, ShouldContain, "1234")
			})
		})

		Convey("When URI does not match any topics", func() {
			importerEventData.URI = "/non-existing-topic"
			updatedImporterEventData := tagImportDataTopics(topicsMap, importerEventData)

			Convey("Then the resulting topics should contain only the default", func() {
				So(updatedImporterEventData.Topics, ShouldResemble, importerEventData.Topics)
				So(updatedImporterEventData.Topics, ShouldHaveLength, len(importerEventData.Topics))
			})
		})

		Convey("When URI segments contain unrelated topics", func() {
			importerEventData.URI = "/economy/environmentalaccounts/non-existing-slug"
			updatedImporterEventData := tagImportDataTopics(topicsMap, importerEventData)

			Convey("Then the resulting topics should contain only related topics", func() {
				expectedTopics := []string{"1234", "6734", "1834"}

				So(updatedImporterEventData.Topics, ShouldHaveLength, len(expectedTopics))
				So(updatedImporterEventData.Topics, ShouldContain, "1234")
				So(updatedImporterEventData.Topics, ShouldContain, "6734")
				So(updatedImporterEventData.Topics, ShouldContain, "1834")
			})
		})
	})
}

func TestAddTopicWithParents(t *testing.T) {
	Convey("Given a set of topics with duplicate slugs and parent relationships", t, func() {
		topicMap := map[string]Topic{
			"4116": {ID: "4116", Slug: "news", ParentSlug: ""},
			"3543": {ID: "3543", Slug: "news", ParentSlug: "news"},
			"1249": {ID: "1249", Slug: "statementsandletters", ParentSlug: "news"},
		}

		Convey("When adding the root topic with slug 'news'", func() {
			uniqueTopics := make(map[string]struct{})
			AddTopicWithParents("news", "", topicMap, uniqueTopics)

			Convey("It should add the root topic only", func() {
				So(uniqueTopics, ShouldContainKey, "4116")
				So(uniqueTopics, ShouldHaveLength, 1)
			})
		})

		Convey("When adding the subtopic with slug 'news' which has a parent 'news", func() {
			uniqueTopics := make(map[string]struct{})
			AddTopicWithParents("news", "news", topicMap, uniqueTopics)

			Convey("It should add both the parent and child topics", func() {
				So(uniqueTopics, ShouldContainKey, "4116")
				So(uniqueTopics, ShouldContainKey, "3543")
				So(uniqueTopics, ShouldHaveLength, 2)
			})
		})

		Convey("When adding the subtopic with slug 'statementsandletters' which has a parent 'news'", func() {
			uniqueTopics := make(map[string]struct{})
			AddTopicWithParents("statementsandletters", "news", topicMap, uniqueTopics)

			Convey("It should add the statementsandletters topic and its parent 'news'", func() {
				So(uniqueTopics, ShouldContainKey, "4116")
				So(uniqueTopics, ShouldContainKey, "1249")
				So(uniqueTopics, ShouldHaveLength, 2)
			})
		})
	})
}
