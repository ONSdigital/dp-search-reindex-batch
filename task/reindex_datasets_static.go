package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sync"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/zebedee"
	"github.com/ONSdigital/dp-search-api/v2/clients"
	"github.com/ONSdigital/dp-search-data-extractor/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"
)

// This pipeline stage takes a channel of datasets (which will have been filtered to be just the static ones) and
// Retrieves the metadata for the latest version of that dataset (as per the Current-Links-LatestVersion uri)
// It does this in a goroutine passing back a channel containing the metadata retreived from the dataset api
func retrieveLatestStaticMetadata(ctx context.Context, tracker *Tracker, datasetClient clients.DatasetAPIClient, datasetChan chan dataset.Dataset, serviceAuthToken string, maxExtractions int) (chan *dataset.Metadata, *sync.WaitGroup) {
	metadataChan := make(chan *dataset.Metadata, defaultChannelBuffer)
	var wg sync.WaitGroup
	go func() {
		defer close(metadataChan)
		for range maxExtractions {
			wg.Add(1)
			go func() {
				for dataset := range datasetChan {
					if dataset.Current == nil {
						log.Warn(ctx, "missing current dataset details", log.Data{"dataset": dataset.ID})
						continue
					}
					id, edition, version, err := getLatestVersionFromURI(ctx, dataset.Current.Links.LatestVersion.URL)
					if err != nil {
						log.Error(ctx, "unable to extract latest version from dataset", err, log.Data{})
						continue
					}
					metadata, err := datasetClient.GetVersionMetadata(ctx, "", serviceAuthToken, "", id, edition, version)
					if err != nil {
						log.Warn(ctx, "failed to retrieve static dataset version metadata", log.Data{
							"err":        err,
							"dataset_id": id,
							"edition":    edition,
							"version":    version,
						})
						continue
					}
					metadataChan <- &metadata
					tracker.Inc("static-metadata")
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}()
	return metadataChan, &wg
}

var versionRegex = regexp.MustCompile(`/datasets/(.+)/editions/(.+)/versions/(.+)`)

// getLatestVersionFromURI is a modified version of `getIDsFromURI` that does not assume a non-versioned path so is more resiliant.
func getLatestVersionFromURI(ctx context.Context, urlString string) (id, edition, version string, err error) {
	versionURL, err := url.Parse(urlString)
	if err != nil {
		log.Error(ctx, "error parsing url", err, log.Data{"url": urlString})
		return "", "", "", err
	}
	path := versionURL.Path
	parts := versionRegex.FindStringSubmatch(path)
	if len(parts) != 4 {
		err = errors.New("invalid latest version path")
		log.Error(ctx, "invalid latest version path", err, log.Data{"url": urlString})
		return "", "", "", err
	}
	return parts[1], parts[2], parts[3], nil
}

// staticMetaDataTransformer is a modified copy of `metaDataTransformer` specifically for static datasets
func staticMetaDataTransformer(ctx context.Context, tracker *Tracker, errChan chan error, metadataChan chan *dataset.Metadata, maxTransforms int, topicsMap map[string]Topic) chan Document {
	transformedChan := make(chan Document, defaultChannelBuffer)
	go func() {
		var wg sync.WaitGroup
		for range maxTransforms {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				transformStaticMetadataDoc(ctx, tracker, errChan, metadataChan, transformedChan, topicsMap)
				wg.Done()
			}(&wg)
		}
		wg.Wait()
		close(transformedChan)
		log.Info(ctx, "finished transforming static metadata")
	}()
	return transformedChan
}

// transformStaticMetadataDoc is a modified copy of `transformMetadataDoc` specifically for static datasets
func transformStaticMetadataDoc(ctx context.Context, tracker *Tracker, errChan chan error, metadataChan chan *dataset.Metadata, transformedChan chan<- Document, topicsMap map[string]Topic) {
	for m := range metadataChan {
		uri := models.GetURI(m)

		parsedURI, err := url.Parse(uri)
		if err != nil {
			log.Error(ctx, "error occurred while parsing url", err)
			errChan <- err
		}

		// Get the topic data
		datasetTopic, err := getDatasetTopic(topicsMap, m)
		if err != nil {
			log.Error(ctx, "error occurred while getting dataset topic", err)
			errChan <- err
			continue
		}

		// Do the mapping - this is separate to the extractor due the different methods of extraction here
		searchDataImport, err := mapStaticDatasetMetadataValues(m, datasetTopic)
		if err != nil {
			log.Error(ctx, "error occurred while mapping static dataset metadata values", err)
			errChan <- err
			continue
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
		tracker.Inc("static-meta-transform")
	}
}

func mapStaticDatasetMetadataValues(metadata *dataset.Metadata, datasetTopic Topic) (searchDataImport *models.SearchDataImport, err error) {
	if metadata == nil {
		return nil, fmt.Errorf("nil metadata cannot be mapped")
	}

	searchDataImport = &models.SearchDataImport{
		DatasetID:       metadata.DatasetDetails.ID,
		DataType:        zebedee.PageTypeDatasetLandingPage,
		Edition:         metadata.EditionTitle,
		MetaDescription: metadata.Description,
		ReleaseDate:     metadata.ReleaseDate,
		Summary:         metadata.Description,
		Title:           metadata.Title,
		Topics:          metadata.Topics,
		UID:             metadata.DatasetDetails.ID,
		URI:             createStaticDatasetURI(datasetTopic.Slug, metadata.DatasetDetails.ID),
	}

	if metadata.Keywords != nil {
		searchDataImport.Keywords = *metadata.Keywords
	}

	return searchDataImport, nil
}

func getDatasetTopic(topicsMap map[string]Topic, metadata *dataset.Metadata) (Topic, error) {
	if metadata == nil || len(metadata.Topics) == 0 {
		return Topic{}, fmt.Errorf("no topics found in metadata")
	}
	topic, ok := topicsMap[metadata.Topics[0]]
	if !ok {
		return Topic{}, fmt.Errorf("topic not found in topics map")
	}
	return topic, nil
}

func createStaticDatasetURI(topicSlug, datasetID string) string {
	return fmt.Sprintf("/%s/datasets/%s", topicSlug, datasetID)
}
