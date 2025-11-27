package task

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	upstreamModels "github.com/ONSdigital/dis-search-upstream-stub/models"
	upstreamStubSDK "github.com/ONSdigital/dis-search-upstream-stub/sdk"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"
)

// getResourceItems gets the specified maximum number of Resources from the upstream service and then puts each Resource item into a channel, which it returns.
func getResourceItems(ctx context.Context, errChan chan error, upstreamStubClient *upstreamStubSDK.Client, maxExtractions int) (resourcesChan chan upstreamModels.Resource) {
	resourcesChan = make(chan upstreamModels.Resource, defaultChannelBuffer)
	go func() {
		defer close(resourcesChan)

		var opts upstreamStubSDK.Options
		opts.Limit(strconv.Itoa(maxExtractions))
		resources, err := upstreamStubClient.GetResources(ctx, opts)
		if err != nil {
			errChan <- err
			log.Error(ctx, "failed to get resources from upstream service", err, log.Data{"options": opts})
			return
		}

		resourceList := resources.Items
		numItems := len(resourceList)

		for i := 0; i < numItems; i++ {
			resourcesChan <- resourceList[i]
		}

		log.Info(ctx, "finished getting resources")
	}()
	return resourcesChan
}

func resourceTransformer(ctx context.Context, tracker *Tracker, errChan chan error, resourceChan chan upstreamModels.Resource, maxTransforms int, topicsMapChan chan map[string]Topic) chan Document {
	var topicsMap map[string]Topic
	for tm := range topicsMapChan {
		topicsMap = tm
	}
	transformedResChan := make(chan Document, defaultChannelBuffer)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < maxTransforms; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				transformResourceItem(ctx, tracker, errChan, resourceChan, transformedResChan, topicsMap)
				wg.Done()
			}(&wg)
		}
		wg.Wait()
		close(transformedResChan)
		log.Info(ctx, "finished transforming resource items")
	}()
	return transformedResChan
}

func transformResourceItem(ctx context.Context, tracker *Tracker, errChan chan error, resourceChan chan upstreamModels.Resource, transformedChan chan<- Document, topicsMap map[string]Topic) {
	for resourceItem := range resourceChan {
		if resourceItem.Title == "" {
			// Don't want to index things without title
			tracker.Inc("untransformed-res-notitle")
			continue // move on to the next resource item
		}
		// Map the data from the Resource into a new exporterEventData object of type dp-search-data-extractor/models.SearchDataImport
		exporterEventData := MapResourceToSearchDataImport(resourceItem)
		// Convert the exporterEventData object into one of type dp-search-data-importer/models.SearchDataImport
		importerEventData := convertToSearchDataModel(exporterEventData)
		if topicsMap != nil {
			importerEventData = tagImportDataTopics(topicsMap, importerEventData)
			if len(importerEventData.Topics) == 0 {
				tracker.Inc("docs-topic-untagged")
				log.Warn(ctx, "untagged topic document",
					log.Data{"URI": importerEventData.URI})
			} else {
				tracker.Inc("docs-topic-tagged")
			}
		}
		esModel := transform.NewTransformer().TransformEventModelToEsModel(&importerEventData)

		body, err := json.Marshal(esModel)
		if err != nil {
			log.Error(ctx, "error marshal to json", err)
			errChan <- err
		}

		transformedDoc := Document{
			ID:   exporterEventData.UID,
			URI:  resourceItem.URI,
			Body: body,
		}
		transformedChan <- transformedDoc
		tracker.Inc("doc-transformed")
	}
}
