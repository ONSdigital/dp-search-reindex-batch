package task

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/ONSdigital/dp-api-clients-go/v2/zebedee"
	"github.com/ONSdigital/dp-search-api/clients"
	extractorModels "github.com/ONSdigital/dp-search-data-extractor/models"
	importerModels "github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	topicCli "github.com/ONSdigital/dp-topic-api/sdk"
	"github.com/ONSdigital/log.go/v2/log"
)

func uriProducer(ctx context.Context, tracker *Tracker, errorChan chan error, z clients.ZebedeeClient) chan string {
	uriChan := make(chan string, defaultChannelBuffer)
	go func() {
		defer close(uriChan)
		items, err := getPublishedURIs(ctx, z)
		if err != nil {
			errorChan <- err
			return
		}
		for _, item := range items {
			// Exclude previous versions of any release (e.g., v1, v2, etc.)
			if strings.Contains(item.URI, "/previous/") {
				log.Info(ctx, "not indexing uri as release is a previous version", log.Data{
					"uri": item.URI,
				})
			} else {
				uriChan <- item.URI
				tracker.Inc("uri")
			}
		}
		log.Info(ctx, "finished listing uris")
	}()
	return uriChan
}

func getPublishedURIs(ctx context.Context, z clients.ZebedeeClient) ([]zebedee.PublishedIndexItem, error) {
	index, err := z.GetPublishedIndex(ctx, &zebedee.PublishedIndexRequestParams{})
	if err != nil {
		log.Error(ctx, "error getting index from zebedee", err)
		return nil, err
	}
	log.Info(ctx, "fetched uris from zebedee", log.Data{"count": index.Count})
	return index.Items, nil
}

func docExtractor(ctx context.Context, tracker *Tracker, errorChan chan error, z clients.ZebedeeClient, uriChan chan string, maxExtractions int) (extractedChan chan Document) {
	extractedChan = make(chan Document, defaultChannelBuffer)
	go func() {
		defer close(extractedChan)

		var wg sync.WaitGroup

		for w := 0; w < maxExtractions; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				extractDoc(ctx, tracker, errorChan, z, uriChan, extractedChan)
			}()
		}
		wg.Wait()
		log.Info(ctx, "finished extracting docs")
	}()
	return
}

func extractDoc(ctx context.Context, tracker *Tracker, errorChan chan error, z clients.ZebedeeClient, uriChan <-chan string, extractedChan chan Document) {
	for uri := range uriChan {
		body, err := z.GetPublishedData(ctx, uri)
		if err != nil {
			errorChan <- err
			log.Error(ctx, "failed to extract doc from zebedee", err, log.Data{"uri": uri})
			continue
		}

		extractedDoc := Document{
			URI:  uri,
			Body: body,
		}
		extractedChan <- extractedDoc
		tracker.Inc("doc-extracted")
	}
}

func docTransformer(ctx context.Context, tracker *Tracker, errChan chan error, extractedChan chan Document, maxTransforms int, topicsMapChan chan map[string]Topic) chan Document {
	var topicsMap map[string]Topic
	for tm := range topicsMapChan {
		topicsMap = tm
	}
	transformedChan := make(chan Document, defaultChannelBuffer)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < maxTransforms; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				transformZebedeeDoc(ctx, tracker, errChan, extractedChan, transformedChan, topicsMap)
				wg.Done()
			}(&wg)
		}
		wg.Wait()
		close(transformedChan)
		log.Info(ctx, "finished transforming zebedee docs")
	}()
	return transformedChan
}

func transformZebedeeDoc(ctx context.Context, tracker *Tracker, errChan chan error, extractedChan chan Document, transformedChan chan<- Document, topicsMap map[string]Topic) {
	for extractedDoc := range extractedChan {
		var zebedeeData extractorModels.ZebedeeData
		err := json.Unmarshal(extractedDoc.Body, &zebedeeData)
		if err != nil {
			log.Error(ctx, "error while attempting to unmarshal zebedee response into zebedeeData", err,
				log.Data{"document": extractedDoc})
			errChan <- err
		}
		if zebedeeData.Description.Title == "" {
			// Don't want to index things without title
			tracker.Inc("untransformed-notitle")
			continue // move on to the next extracted doc
		}

		if !isEditorialSeries(zebedeeData.DataType) && zebedeeData.Description.MigrationLink != "" {
			// Do not index if not editorial series and has migrationLink
			log.Info(ctx, "content migrated - not indexing", log.Data{
				"uri": zebedeeData.URI,
			})
			tracker.Inc("doc-migrated")
			continue
		}

		exporterEventData := extractorModels.MapZebedeeDataToSearchDataImport(zebedeeData, -1)
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
			URI:  extractedDoc.URI,
			Body: body,
		}
		transformedChan <- transformedDoc
		tracker.Inc("doc-transformed")
	}
}

func retrieveTopicsMap(ctx context.Context, errorChan chan error, enabled bool, serviceAuthToken string, topicClient topicCli.Clienter) chan map[string]Topic {
	topicsMapChan := make(chan map[string]Topic, 1)

	go func() {
		defer close(topicsMapChan)
		if enabled {
			topicsMap, err := LoadTopicsMap(ctx, serviceAuthToken, topicClient)
			if err != nil {
				errorChan <- err
				return
			}
			topicsMapChan <- topicsMap
			log.Info(ctx, "finished retrieving topics map", log.Data{"map_size": len(topicsMap)})
		} else {
			log.Info(ctx, "topic map retrieval disabled")
		}
	}()

	return topicsMapChan
}

// Auto tag import data topics based on the URI segments of the request
func tagImportDataTopics(topicsMap map[string]Topic, importerEventData importerModels.SearchDataImport) importerModels.SearchDataImport {
	// Set to track unique topic IDs
	uniqueTopics := make(map[string]struct{})

	// Add existing topics in searchData.Topics
	for _, topicID := range importerEventData.Topics {
		if _, exists := uniqueTopics[topicID]; !exists {
			uniqueTopics[topicID] = struct{}{}
		}
	}

	// Break URI into segments and exclude the last segment
	uriSegments := strings.Split(importerEventData.URI, "/")

	// Add topics based on URI segments, starting from root or contextually relevant topic
	parentSlug := ""
	for _, segment := range uriSegments {
		AddTopicWithParents(segment, parentSlug, topicsMap, uniqueTopics)
		parentSlug = segment // Update parentSlug for the next iteration
	}

	// Convert set to slice
	importerEventData.Topics = make([]string, 0, len(uniqueTopics))
	for topicID := range uniqueTopics {
		importerEventData.Topics = append(importerEventData.Topics, topicID)
	}

	return importerEventData
}

// AddTopicWithParents adds a topic and its parents to the uniqueTopics map if they don't already exist.
// It recursively adds parent topics until it reaches the root topic.
func AddTopicWithParents(slug, parentSlug string, topicsMap map[string]Topic, uniqueTopics map[string]struct{}) {
	for _, topic := range topicsMap {
		// Find the topic by matching both slug and parentSlug
		if topic.Slug == slug && topic.ParentSlug == parentSlug {
			if _, alreadyProcessed := uniqueTopics[topic.ID]; !alreadyProcessed {
				uniqueTopics[topic.ID] = struct{}{}
				if topic.ParentSlug != "" {
					// Recursively add the parent topic
					AddTopicWithParents(topic.ParentSlug, "", topicsMap, uniqueTopics)
				}
			}
			return // Stop processing once the correct topic is found and added
		}
	}
}

func isEditorialSeries(contentType string) bool {
	editorialSeries := map[string]bool{
		"bulletin":                true,
		"article":                 true,
		"compendium_landing_page": true,
	}
	return editorialSeries[contentType]
}
