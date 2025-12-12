package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-search-api/clients/mock"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/smartystreets/goconvey/convey"
)

func TestExtractDatasets(t *testing.T) {
	convey.Convey("Given a dataset client that succeeds to return up to 3 datasets", t, func() {
		items := []dataset.Dataset{
			{ID: "dataset1"},
			{ID: "dataset2"},
			{ID: "dataset3"},
		}
		cli := &mock.DatasetAPIClientMock{
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

		convey.Convey("Then extractDatasets with a paginationLimit of 2 send all the datasets to the dataset channel", func() {
			datasetChan, wg := extractDatasets(ctx, tracker, nil, cli, testAuthToken, 2)

			ds1 := <-datasetChan
			convey.So(ds1, convey.ShouldResemble, dataset.Dataset{ID: "dataset1"})
			ds2 := <-datasetChan
			convey.So(ds2, convey.ShouldResemble, dataset.Dataset{ID: "dataset2"})
			ds3 := <-datasetChan
			convey.So(ds3, convey.ShouldResemble, dataset.Dataset{ID: "dataset3"})
			wg.Wait()

			convey.Convey("And dataset api has been called twice with the expected pagination parameters", func() {
				convey.So(cli.GetDatasetsCalls(), convey.ShouldHaveLength, 2)
				convey.So(cli.GetDatasetsCalls()[0].Q.Offset, convey.ShouldEqual, 0)
				convey.So(cli.GetDatasetsCalls()[0].Q.Limit, convey.ShouldEqual, 2)
				convey.So(cli.GetDatasetsCalls()[0].ServiceAuthToken, convey.ShouldEqual, testAuthToken)
				convey.So(cli.GetDatasetsCalls()[1].Q.Offset, convey.ShouldEqual, 2)
				convey.So(cli.GetDatasetsCalls()[1].Q.Limit, convey.ShouldEqual, 2)
				convey.So(cli.GetDatasetsCalls()[1].ServiceAuthToken, convey.ShouldEqual, testAuthToken)
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

	convey.Convey("Given a dataset client that succeeds to return multiple editions where only one has ID and link, and a datasetChan channel", t, func() {
		cli := &mock.DatasetAPIClientMock{
			GetFullEditionsDetailsFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, datasetID string) ([]dataset.EditionsDetails, error) {
				return testEditionDetails, nil
			},
		}
		datasetChan := make(chan dataset.Dataset, 1)
		tracker := &Tracker{}

		convey.Convey("When a valid dataset is sent to the dataset channel and consumed by retrieveDatasetEditions", func() {
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

			convey.Convey("Then the expected editions and isBasedOn are sent to the edition channel returned by retrieveDatasetEditions", func() {
				ed1 := <-editionChan
				convey.So(ed1, convey.ShouldResemble, DatasetEditionMetadata{
					id:        testDatasetID,
					editionID: testEdition,
					version:   testVersion,
				})

				convey.Convey("And the expected call is performed against dataset api", func() {
					convey.So(cli.GetFullEditionsDetailsCalls(), convey.ShouldHaveLength, 1)
					convey.So(cli.GetFullEditionsDetailsCalls()[0].DatasetID, convey.ShouldEqual, testDatasetID)
					convey.So(cli.GetFullEditionsDetailsCalls()[0].CollectionID, convey.ShouldEqual, testCollectionID)
					convey.So(cli.GetFullEditionsDetailsCalls()[0].ServiceAuthToken, convey.ShouldEqual, testAuthToken)
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

	convey.Convey("Given a dataset client that succeeds to return valid metadata and an editionMetadata channel", t, func() {
		cli := &mock.DatasetAPIClientMock{
			GetVersionMetadataFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, id string, edition string, version string) (dataset.Metadata, error) {
				return testMetadata, nil
			},
		}
		editionMetadata := make(chan DatasetEditionMetadata, 1)
		tracker := &Tracker{}

		convey.Convey("When a dataset edition metadata is sent to the edition metadata channel and consumed by retrieveLatestMetadata", func() {
			editionMetadata <- DatasetEditionMetadata{
				id:        testDatasetID,
				editionID: testEdition,
				version:   testVersion,
			}
			close(editionMetadata)

			metadataChan, _ := retrieveLatestMetadata(ctx, tracker, cli, editionMetadata, testAuthToken, 10)

			convey.Convey("Then the expected metadata and isBasedOn are sent to the metadataChannel", func() {
				m := <-metadataChan
				convey.So(m, convey.ShouldResemble, &testMetadata)

				convey.Convey("And the expected call is performed against dataset api", func() {
					convey.So(cli.GetVersionMetadataCalls(), convey.ShouldHaveLength, 1)
					convey.So(cli.GetVersionMetadataCalls()[0].ID, convey.ShouldEqual, testDatasetID)
					convey.So(cli.GetVersionMetadataCalls()[0].Edition, convey.ShouldEqual, testEdition)
					convey.So(cli.GetVersionMetadataCalls()[0].Version, convey.ShouldEqual, testVersion)
					convey.So(cli.GetVersionMetadataCalls()[0].ServiceAuthToken, convey.ShouldEqual, testAuthToken)
				})
			})
		})
	})
}

func TestTransformMetadataDoc(t *testing.T) {
	convey.Convey("Given a metadata channel and a transformed document channel", t, func() {
		metadataChan := make(chan *dataset.Metadata, 1)
		transformedChan := make(chan Document, 1)
		tracker := &Tracker{}

		convey.Convey("When a generic dataset metadata is sent to the channel and consumed by transformMetadataDoc", func() {
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

			expected := &models.EsModel{
				DataType:       "dataset_landing_page",
				URI:            testURI,
				DatasetID:      testDatasetID,
				Edition:        testEdition,
				PopulationType: &models.EsPopulationType{},
			}

			metadataChan <- sent
			close(metadataChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func(waitGroup *sync.WaitGroup) {
				transformMetadataDoc(ctx, tracker, nil, metadataChan, transformedChan)
				wg.Done()
			}(wg)

			convey.Convey("Then the expected elasticsearch document is sent to the transformed channel", func() {
				transformed := <-transformedChan
				convey.So(transformed.ID, convey.ShouldEqual, testDatasetID)
				convey.So(transformed.URI, convey.ShouldEqual, testURI)

				esModel := &models.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				convey.So(err, convey.ShouldBeNil)
				convey.So(esModel, convey.ShouldResemble, expected)

				wg.Wait()
			})
		})
	})

	convey.Convey("Given a metadata channel and a transformed document channel", t, func() {
		metadataChan := make(chan *dataset.Metadata, 1)
		transformedChan := make(chan Document, 1)
		tracker := &Tracker{}

		convey.Convey("When a cantabular type dataset metadata is sent to the channel and consumed by transformMetadataDoc", func() {
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

			expected := &models.EsModel{
				DataType:  "dataset_landing_page", // dataset_landing_page type is used for cantabular types
				URI:       testURI,
				DatasetID: testDatasetID,
				Edition:   testEdition,
				PopulationType: &models.EsPopulationType{
					Key:    "all-usual-residents-in-households",
					AggKey: "all-usual-residents-in-households###All usual residents in households",
					Name:   "UR_HH",
					Label:  "All usual residents in households",
				},
				Dimensions: []models.EsDimension{
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

			convey.Convey("Then the expected elasticsearch document is sent to the transformed channel", func() {
				transformed := <-transformedChan
				convey.So(transformed.ID, convey.ShouldEqual, testDatasetID)
				convey.So(transformed.URI, convey.ShouldEqual, testURI)

				esModel := &models.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				convey.So(err, convey.ShouldBeNil)
				convey.So(esModel.DataType, convey.ShouldEqual, expected.DataType)
				convey.So(esModel.URI, convey.ShouldEqual, expected.URI)
				convey.So(esModel.DatasetID, convey.ShouldEqual, expected.DatasetID)
				convey.So(esModel.Edition, convey.ShouldEqual, expected.Edition)
				convey.So(esModel.PopulationType, convey.ShouldResemble, expected.PopulationType)
				convey.So(esModel.Dimensions, convey.ShouldHaveLength, len(expected.Dimensions))
				for _, dim := range expected.Dimensions {
					convey.So(esModel.Dimensions, convey.ShouldContain, dim)
				}

				wg.Wait()
			})
		})
	})
}
