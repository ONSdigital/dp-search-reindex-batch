package task

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/ONSdigital/dis-search-upstream-stub/models"
	"github.com/ONSdigital/dis-search-upstream-stub/sdk"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"
)

// resourceGetter starts a go routine which gets the specified maximum number of Resources from the upstream service and
// then puts each Resource item into a channel, which it returns.
func resourceGetter(ctx context.Context, tracker *Tracker, errChan chan error, upstreamStubClient *sdk.Client, pageLimit, maxExtractions int) (resourcesChan chan models.SearchContentUpdatedResource) {
	resourcesChan = make(chan models.SearchContentUpdatedResource, defaultChannelBuffer)
	go func() {
		defer close(resourcesChan)
		offsetChan := make(chan int, 1)
		totalCountChan := make(chan int)
		defer close(totalCountChan)
		go func() {
			defer close(offsetChan)
			offsetChan <- 0                // trigger first page fetch
			totalCount := <-totalCountChan // wait for the total_count to be returned from first fetch
			log.Info(ctx, "total upstream count", log.Data{"total_count": totalCount})
			for n := pageLimit; n < totalCount; n += pageLimit {
				offsetChan <- n // trigger subsequent pages
			}
		}()

		var wg sync.WaitGroup
		for i := 0; i < maxExtractions; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for offset := range offsetChan {
					resources, err := getResourcePage(ctx, upstreamStubClient, pageLimit, offset)
					if err != nil {
						errChan <- err
						log.Error(ctx, "failed to get page of resources from upstream service", err, log.Data{"limit": pageLimit, "offset": offset})
						continue
					}
					// if first page, return the total count so subsequent pages can be triggered
					if offset == 0 {
						totalCountChan <- resources.TotalCount
					}
					processResourcesPage(ctx, tracker, resourcesChan, resources)
				}
			}(&wg)
		}
		wg.Wait()
		log.Info(ctx, "finished getting resources from upstream")
	}()
	return resourcesChan
}

func getResourcePage(ctx context.Context, upstreamStubClient *sdk.Client, limit, offset int) (*models.Resources, error) {
	var opts sdk.Options
	opts.Limit(strconv.Itoa(limit))
	opts.Offset(strconv.Itoa(offset))
	log.Info(ctx, "getting page of resources from upstream", log.Data{"limit": limit, "offset": offset})
	res, err := upstreamStubClient.GetResources(ctx, opts)
	return res, err
}

func processResourcesPage(ctx context.Context, tracker *Tracker, resourcesChan chan models.SearchContentUpdatedResource, resources *models.Resources) {
	for _, res := range resources.Items {
		scur, ok := res.(models.SearchContentUpdatedResource)
		if ok {
			resourcesChan <- scur
			tracker.Inc("upstream-resources")
		} else {
			log.Info(ctx, "unexpected resource type returned from upstream", log.Data{"type": res.GetResourceType()})
			tracker.Inc("upstream-resources-unknown-type")
		}
	}
}

func resourceTransformer(ctx context.Context, tracker *Tracker, errChan chan error, resourceChan chan models.SearchContentUpdatedResource, maxTransforms int, topicsMapChan chan map[string]Topic) chan Document {
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

func transformResourceItem(ctx context.Context, tracker *Tracker, errChan chan error, resourceChan chan models.SearchContentUpdatedResource, transformedChan chan<- Document, topicsMap map[string]Topic) {
	for resourceItem := range resourceChan {
		if resourceItem.Title == "" {
			// Don't want to index things without title
			tracker.Inc("upstream-resources-untransformed-res-no-title")
			continue // move on to the next resource item
		}
		// Map the data from the Resource into a new exporterEventData object of type dp-search-data-extractor/models.SearchDataImport
		exporterEventData := MapResourceToSearchDataImport(resourceItem)
		// Convert the exporterEventData object into one of type dp-search-data-importer/models.SearchDataImport
		importerEventData := convertToSearchDataModel(exporterEventData)
		if topicsMap != nil {
			importerEventData = tagImportDataTopics(topicsMap, importerEventData)
			if len(importerEventData.Topics) == 0 {
				tracker.Inc("upstream-resources-untagged")
				log.Warn(ctx, "untagged topic document",
					log.Data{"URI": importerEventData.URI})
			} else {
				tracker.Inc("upstream-resources-topic-tagged")
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
		tracker.Inc("upstream-resources-transformed")
	}
}
