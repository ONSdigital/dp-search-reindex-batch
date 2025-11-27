package task

import (
	"context"
	"encoding/json"
	"net/url"
	"sync"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-search-api/clients"
	"github.com/ONSdigital/dp-search-data-extractor/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"
)

// DatasetEditionMetadata holds the necessary information for a dataset edition, plus isBasedOn
type DatasetEditionMetadata struct {
	id        string
	editionID string
	version   string
}

func extractDatasets(ctx context.Context, tracker *Tracker, errChan chan error, datasetClient clients.DatasetAPIClient, serviceAuthToken string, paginationLimit int) (chan dataset.Dataset, *sync.WaitGroup) {
	datasetChan := make(chan dataset.Dataset, defaultChannelBuffer)
	var wg sync.WaitGroup

	// extractAll extracts all datasets from datasetAPI in batches of up to 'PaginationLimit' size
	extractAll := func() {
		defer func() {
			close(datasetChan)
			wg.Done()
		}()
		var list dataset.List
		var err error
		var offset = 0
		for {
			list, err = datasetClient.GetDatasets(ctx, "", serviceAuthToken, "", &dataset.QueryParams{
				Offset: offset,
				Limit:  paginationLimit,
			})
			if err != nil {
				log.Error(ctx, "error retrieving datasets", err)
				errChan <- err
			}
			log.Info(ctx, "got datasets batch", log.Data{
				"count":       list.Count,
				"total_count": list.TotalCount,
				"offset":      list.Offset,
			})

			if len(list.Items) == 0 {
				break
			}
			for i := 0; i < len(list.Items); i++ {
				datasetChan <- list.Items[i]
				tracker.Inc("dataset-extracted")
			}
			offset += paginationLimit

			if offset > list.TotalCount {
				break
			}
		}
	}

	wg.Add(1)
	go extractAll()

	return datasetChan, &wg
}

func retrieveDatasetEditions(ctx context.Context, tracker *Tracker, datasetClient clients.DatasetAPIClient, datasetChan chan dataset.Dataset, serviceAuthToken string, maxExtractions int) (chan DatasetEditionMetadata, *sync.WaitGroup) {
	editionMetadataChan := make(chan DatasetEditionMetadata, defaultChannelBuffer)
	var wg sync.WaitGroup
	go func() {
		defer close(editionMetadataChan)
		for i := 0; i < maxExtractions; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for dataSet := range datasetChan {
					if dataSet.Current == nil {
						continue
					}
					editions, err := datasetClient.GetFullEditionsDetails(ctx, "", serviceAuthToken, dataSet.CollectionID, dataSet.Current.ID)
					if err != nil {
						log.Warn(ctx, "error retrieving editions", log.Data{
							"err":           err,
							"dataset_id":    dataSet.Current.ID,
							"collection_id": dataSet.CollectionID,
						})
						continue
					}
					for i := 0; i < len(editions); i++ {
						if editions[i].ID == "" || editions[i].Current.Links.LatestVersion.ID == "" {
							continue
						}
						editionMetadataChan <- DatasetEditionMetadata{
							id:        dataSet.Current.ID,
							editionID: editions[i].Current.Edition,
							version:   editions[i].Current.Links.LatestVersion.ID,
						}
						tracker.Inc("editions")
					}
				}
			}()
		}
		wg.Wait()
	}()
	return editionMetadataChan, &wg
}

func retrieveLatestMetadata(ctx context.Context, tracker *Tracker, datasetClient clients.DatasetAPIClient, editionMetadata chan DatasetEditionMetadata, serviceAuthToken string, maxExtractions int) (chan *dataset.Metadata, *sync.WaitGroup) {
	metadataChan := make(chan *dataset.Metadata, defaultChannelBuffer)
	var wg sync.WaitGroup
	go func() {
		defer close(metadataChan)
		for i := 0; i < maxExtractions; i++ {
			wg.Add(1)
			go func() {
				for edMetadata := range editionMetadata {
					metadata, err := datasetClient.GetVersionMetadata(ctx, "", serviceAuthToken, "", edMetadata.id, edMetadata.editionID, edMetadata.version)
					if err != nil {
						log.Warn(ctx, "failed to retrieve dataset version metadata", log.Data{
							"err":        err,
							"dataset_id": edMetadata.id,
							"edition":    edMetadata.editionID,
							"version":    edMetadata.version,
						})
						continue
					}
					metadataChan <- &metadata
					tracker.Inc("metadata")
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}()
	return metadataChan, &wg
}

func metaDataTransformer(ctx context.Context, tracker *Tracker, errChan chan error, metadataChan chan *dataset.Metadata, maxTransforms int) chan Document {
	transformedChan := make(chan Document, defaultChannelBuffer)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < maxTransforms; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				transformMetadataDoc(ctx, tracker, errChan, metadataChan, transformedChan)
				wg.Done()
			}(&wg)
		}
		wg.Wait()
		close(transformedChan)
		log.Info(ctx, "finished transforming metadata")
	}()
	return transformedChan
}

func transformMetadataDoc(ctx context.Context, tracker *Tracker, errChan chan error, metadataChan chan *dataset.Metadata, transformedChan chan<- Document) {
	for m := range metadataChan {
		uri := models.GetURI(m)

		parsedURI, err := url.Parse(uri)
		if err != nil {
			log.Error(ctx, "error occurred while parsing url", err)
			errChan <- err
		}

		datasetID, edition, _, getIDErr := getIDsFromURI(uri)
		if getIDErr != nil {
			datasetID = m.DatasetDetails.ID
			edition = m.DatasetDetails.Links.Edition.ID
		}

		searchDataImport := &models.SearchDataImport{
			UID:       m.DatasetDetails.ID,
			URI:       parsedURI.Path,
			Edition:   edition,
			DatasetID: datasetID,
			DataType:  "dataset_landing_page",
		}

		if err = searchDataImport.MapDatasetMetadataValues(context.Background(), m); err != nil {
			log.Error(ctx, "error occurred while mapping dataset metadata values", err)
			errChan <- err
		}

		importerEventData := convertToSearchDataModel(*searchDataImport)
		esModel := transform.NewTransformer().TransformEventModelToEsModel(&importerEventData)
		body, err := json.Marshal(esModel)
		if err != nil {
			log.Error(ctx, "error marshal to json", err)
			errChan <- err
		}

		transformedDoc := Document{
			ID:   searchDataImport.UID,
			URI:  parsedURI.Path,
			Body: body,
		}
		transformedChan <- transformedDoc
		tracker.Inc("meta-transform")
	}
}
