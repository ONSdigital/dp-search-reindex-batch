package task

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-search-api/v2/clients/mock"
	importerModels "github.com/ONSdigital/dp-search-data-importer/models"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testTopic = Topic{
		ID:   "topic-id",
		Slug: "economy",
	}
	testTopicsMap = map[string]Topic{
		testTopic.ID: testTopic,
	}
)

func TestGetLatestVersionFromURI(t *testing.T) {
	Convey("Given a latest version dataset URL", t, func() {
		Convey("When the URL path matches the version pattern", func() {
			id, edition, version, err := getLatestVersionFromURI(ctx, "http://test-host:1234/datasets/TS056/editions/2021/versions/4")

			Convey("Then the dataset ID, edition and version are extracted", func() {
				So(err, ShouldBeNil)
				So(id, ShouldEqual, testDatasetID)
				So(edition, ShouldEqual, testEdition)
				So(version, ShouldEqual, testVersion)
			})
		})

		Convey("When the URL path does not match the version pattern", func() {
			id, edition, version, err := getLatestVersionFromURI(ctx, "http://test-host:1234/datasets/TS056")

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "invalid latest version path")
				So(id, ShouldBeBlank)
				So(edition, ShouldBeBlank)
				So(version, ShouldBeBlank)
			})
		})
	})
}

func TestRetrieveLatestStaticMetadata(t *testing.T) {
	Convey("Given a static dataset and a dataset client that returns metadata", t, func() {
		expectedMetadata := dataset.Metadata{
			DatasetDetails: dataset.DatasetDetails{
				ID: testDatasetID,
			},
		}
		cli := &mock.DatasetAPIClientMock{
			GetVersionMetadataFunc: func(ctxContext context.Context, userAuthToken string, serviceAuthToken string, collectionID string, id string, edition string, version string) (dataset.Metadata, error) {
				return expectedMetadata, nil
			},
		}
		datasetChan := make(chan dataset.Dataset, 1)
		tracker := &Tracker{}

		Convey("When retrieveLatestStaticMetadata consumes the dataset", func() {
			datasetChan <- dataset.Dataset{
				ID: testDatasetID,
				Current: &dataset.DatasetDetails{
					ID: testDatasetID,
					Links: dataset.Links{
						LatestVersion: dataset.Link{
							URL: "http://test-host:1234/datasets/TS056/editions/2021/versions/4",
						},
					},
				},
			}
			close(datasetChan)

			metadataChan, wg := retrieveLatestStaticMetadata(ctx, tracker, cli, datasetChan, testAuthToken, 1)

			Convey("Then it fetches and emits the latest version metadata", func() {
				metadata := <-metadataChan
				So(metadata, ShouldResemble, &expectedMetadata)

				wg.Wait()
				So(cli.GetVersionMetadataCalls(), ShouldHaveLength, 1)
				So(cli.GetVersionMetadataCalls()[0].ID, ShouldEqual, testDatasetID)
				So(cli.GetVersionMetadataCalls()[0].Edition, ShouldEqual, testEdition)
				So(cli.GetVersionMetadataCalls()[0].Version, ShouldEqual, testVersion)
				So(cli.GetVersionMetadataCalls()[0].ServiceAuthToken, ShouldEqual, testAuthToken)
				So(tracker.Get()["static-metadata"], ShouldEqual, 1)
			})
		})
	})
}

func TestTransformStaticMetadataDoc(t *testing.T) {
	Convey("Given static dataset metadata with an injected topic mapping", t, func() {
		metadataChan := make(chan *dataset.Metadata, 1)
		transformedChan := make(chan Document, 1)
		tracker := &Tracker{}

		Convey("When transformStaticMetadataDoc consumes the metadata", func() {
			metadataChan <- &dataset.Metadata{
				Version: dataset.Version{
					EditionTitle: "static-edition",
				},
				DatasetLinks: dataset.Links{
					LatestVersion: dataset.Link{
						URL: "http://test-host:1234/datasets/static-dataset",
					},
				},
				DatasetDetails: dataset.DatasetDetails{
					ID:     testDatasetID,
					Topics: []string{testTopic.ID},
				},
			}
			close(metadataChan)

			transformStaticMetadataDoc(ctx, tracker, nil, metadataChan, transformedChan, testTopicsMap)

			Convey("Then it keeps the source document URI and injects the topic slug into the indexed URI", func() {
				transformed := <-transformedChan
				So(transformed.ID, ShouldEqual, testDatasetID)
				So(transformed.URI, ShouldEqual, "/datasets/static-dataset")

				esModel := &importerModels.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				So(err, ShouldBeNil)
				So(esModel.DataType, ShouldEqual, "dataset_landing_page")
				So(esModel.URI, ShouldEqual, "/"+testTopic.Slug+"/datasets/TS056")
				So(esModel.DatasetID, ShouldEqual, testDatasetID)
				So(esModel.Edition, ShouldEqual, "static-edition")
				So(tracker.Get()["static-meta-transform"], ShouldEqual, 1)
			})
		})
	})

	Convey("Given static dataset metadata with no topics", t, func() {
		metadataChan := make(chan *dataset.Metadata, 1)
		transformedChan := make(chan Document, 1)
		errChan := make(chan error, 1)
		tracker := &Tracker{}

		Convey("When transformStaticMetadataDoc consumes the metadata", func() {
			metadataChan <- &dataset.Metadata{
				Version: dataset.Version{
					EditionTitle: "static-edition",
				},
				DatasetLinks: dataset.Links{
					LatestVersion: dataset.Link{
						URL: "http://test-host:1234/datasets/static-dataset",
					},
				},
				DatasetDetails: dataset.DatasetDetails{
					ID: testDatasetID,
				},
			}
			close(metadataChan)

			transformStaticMetadataDoc(ctx, tracker, errChan, metadataChan, transformedChan, testTopicsMap)

			Convey("Then it returns an error and does not emit a transformed document", func() {
				err := <-errChan
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "no topics found in metadata")

				select {
				case transformed := <-transformedChan:
					So(transformed, ShouldResemble, Document{})
				default:
				}

				So(tracker.Get()["static-meta-transform"], ShouldEqual, 0)
			})
		})
	})
}

func TestMapStaticDatasetMetadataValues(t *testing.T) {
	Convey("Given static dataset metadata and a resolved dataset topic", t, func() {
		keywords := []string{"population", "employment"}
		metadata := &dataset.Metadata{
			Version: dataset.Version{
				EditionTitle: "static-edition",
				ReleaseDate:  "2024-01-02",
			},
			DatasetDetails: dataset.DatasetDetails{
				ID:          testDatasetID,
				Description: "dataset description",
				Title:       "dataset title",
				Keywords:    &keywords,
				Topics:      []string{testTopic.ID},
			},
		}

		Convey("When mapStaticDatasetMetadataValues is called", func() {
			mapped, err := mapStaticDatasetMetadataValues(metadata, testTopic)

			Convey("Then it maps the dataset metadata into the importer model", func() {
				So(err, ShouldBeNil)
				So(mapped.DatasetID, ShouldEqual, testDatasetID)
				So(mapped.DataType, ShouldEqual, "dataset_landing_page")
				So(mapped.Edition, ShouldEqual, "static-edition")
				So(mapped.MetaDescription, ShouldEqual, "dataset description")
				So(mapped.ReleaseDate, ShouldEqual, "2024-01-02")
				So(mapped.Summary, ShouldEqual, "dataset description")
				So(mapped.Title, ShouldEqual, "dataset title")
				So(mapped.Topics, ShouldResemble, []string{testTopic.ID})
				So(mapped.UID, ShouldEqual, testDatasetID)
				So(mapped.URI, ShouldEqual, "/economy/datasets/TS056")
				So(mapped.Keywords, ShouldResemble, keywords)
			})
		})

		Convey("When mapStaticDatasetMetadataValues is called with nil metadata", func() {
			mapped, err := mapStaticDatasetMetadataValues(nil, testTopic)

			Convey("Then it returns an error", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "nil metadata cannot be mapped")
				So(mapped, ShouldBeNil)
			})
		})
	})
}

func TestGetDatasetTopic(t *testing.T) {
	Convey("Given metadata containing a known topic ID", t, func() {
		metadata := &dataset.Metadata{
			DatasetDetails: dataset.DatasetDetails{
				Topics: []string{testTopic.ID},
			},
		}

		Convey("When getDatasetTopic is called", func() {
			topic, err := getDatasetTopic(testTopicsMap, metadata)

			Convey("Then it returns the matching topic", func() {
				So(err, ShouldBeNil)
				So(topic, ShouldResemble, testTopic)
			})
		})
	})

	Convey("Given metadata with no topics", t, func() {
		metadata := &dataset.Metadata{}

		Convey("When getDatasetTopic is called", func() {
			topic, err := getDatasetTopic(testTopicsMap, metadata)

			Convey("Then it returns a no topics error", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "no topics found in metadata")
				So(topic, ShouldResemble, Topic{})
			})
		})
	})

	Convey("Given metadata containing a topic that is not in the map", t, func() {
		metadata := &dataset.Metadata{
			DatasetDetails: dataset.DatasetDetails{
				Topics: []string{"unknown-topic"},
			},
		}

		Convey("When getDatasetTopic is called", func() {
			topic, err := getDatasetTopic(testTopicsMap, metadata)

			Convey("Then it returns a missing topic error", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "topic not found in topics map")
				So(topic, ShouldResemble, Topic{})
			})
		})
	})
}

func TestCreateStaticDatasetURI(t *testing.T) {
	Convey("Given a topic slug and dataset ID", t, func() {
		Convey("When createStaticDatasetURI is called", func() {
			uri := createStaticDatasetURI(testTopic.Slug, testDatasetID)

			Convey("Then it returns the topic-prefixed dataset URI", func() {
				So(uri, ShouldEqual, "/economy/datasets/TS056")
			})
		})
	})
}
