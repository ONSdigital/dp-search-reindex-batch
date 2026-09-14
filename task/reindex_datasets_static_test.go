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
	Convey("Given static dataset metadata whose URI is not versioned", t, func() {
		metadataChan := make(chan *dataset.Metadata, 1)
		transformedChan := make(chan Document, 1)
		tracker := &Tracker{}
		editionFromMetadata := "static-edition"

		Convey("When transformStaticMetadataDoc consumes the metadata", func() {
			metadataChan <- &dataset.Metadata{
				DatasetLinks: dataset.Links{
					LatestVersion: dataset.Link{
						URL: "http://test-host:1234/datasets/static-dataset",
					},
				},
				DatasetDetails: dataset.DatasetDetails{
					ID: testDatasetID,
					Links: dataset.Links{
						Edition: dataset.Link{ID: editionFromMetadata},
					},
				},
			}
			close(metadataChan)

			transformStaticMetadataDoc(ctx, tracker, nil, metadataChan, transformedChan)

			Convey("Then it falls back to metadata fields for dataset ID and edition", func() {
				transformed := <-transformedChan
				So(transformed.ID, ShouldEqual, testDatasetID)
				So(transformed.URI, ShouldEqual, "/datasets/static-dataset")

				esModel := &importerModels.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				So(err, ShouldBeNil)
				So(esModel.DataType, ShouldEqual, "dataset_landing_page")
				So(esModel.URI, ShouldEqual, "/datasets/static-dataset")
				So(esModel.DatasetID, ShouldEqual, testDatasetID)
				So(esModel.Edition, ShouldEqual, editionFromMetadata)
				So(tracker.Get()["static-meta-transform"], ShouldEqual, 1)
			})
		})
	})
}
