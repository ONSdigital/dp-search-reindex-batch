package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	upstreamModels "github.com/ONSdigital/dis-search-upstream-stub/models"
	upstreamStubSDK "github.com/ONSdigital/dis-search-upstream-stub/sdk"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/zebedee"
	dpEs "github.com/ONSdigital/dp-elasticsearch/v3"
	dpEsClient "github.com/ONSdigital/dp-elasticsearch/v3/client"
	v710 "github.com/ONSdigital/dp-elasticsearch/v3/client/elasticsearch/v710"
	"github.com/ONSdigital/dp-net/v2/awsauth"
	dphttp2 "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/dp-search-api/clients"
	"github.com/ONSdigital/dp-search-api/elasticsearch"
	extractorModels "github.com/ONSdigital/dp-search-data-extractor/models"
	importerModels "github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/dp-search-reindex-batch/config"
	topicCli "github.com/ONSdigital/dp-topic-api/sdk"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

const (
	defaultChannelBuffer = 20
	awsESService         = "es"
)

// DatasetEditionMetadata holds the necessary information for a dataset edition, plus isBasedOn
type DatasetEditionMetadata struct {
	id        string
	editionID string
	version   string
}

type Document struct {
	ID   string
	URI  string
	Body []byte
}

func reindex(ctx context.Context, cfg *config.Config) error {
	log.Info(ctx, "running reindex script", log.Data{"config": cfg})

	hcClienter := dphttp2.NewClient()
	if hcClienter == nil {
		err := errors.New("failed to create dp http client")
		log.Error(ctx, err.Error(), err)
		return err
	}
	hcClienter.SetMaxRetries(2)
	hcClienter.SetTimeout(cfg.ZebedeeTimeout)

	zebClient := zebedee.NewClientWithClienter(cfg.ZebedeeURL, hcClienter)
	if zebClient == nil {
		err := errors.New("failed to create zebedee client")
		log.Error(ctx, err.Error(), err)
		return err
	}

	esHTTPClient := hcClienter
	if cfg.SignESRequests {
		log.Info(ctx, "use a signing roundtripper client")
		awsSignerRT, err := awsauth.NewAWSSignerRoundTripper("", "", cfg.AwsRegion, awsESService,
			awsauth.Options{TlsInsecureSkipVerify: cfg.AwsSecSkipVerify})
		if err != nil {
			log.Error(ctx, "failed to create http signer", err)
			return err
		}

		esHTTPClient = dphttp2.NewClientWithTransport(awsSignerRT)
	}

	datasetClient := dataset.NewAPIClient(cfg.DatasetAPIURL)
	esClient, esClientErr := dpEs.NewClient(dpEsClient.Config{
		ClientLib: dpEsClient.GoElasticV710,
		Address:   cfg.ElasticSearchURL,
		Transport: esHTTPClient,
	})
	if esClientErr != nil {
		log.Error(ctx, "failed to create dp-elasticsearch client", esClientErr)
		return esClientErr
	}

	if err := esClient.NewBulkIndexer(ctx); err != nil {
		log.Error(ctx, "failed to create new bulk indexer", err)
		return err
	}

	topicClient := topicCli.New(cfg.TopicAPIURL)
	if topicClient == nil {
		err := errors.New("failed to create topic client")
		log.Error(ctx, err.Error(), err)
		return err
	}

	// Create a new client for each url and endpoint pair in the OtherUpstreamServices array
	// Add each new client to an array of clients
	numUpstreamServices := len(cfg.OtherUpstreamServices)
	upstreamServiceClients := make([]*upstreamStubSDK.Client, numUpstreamServices)

	for i := 0; i < numUpstreamServices; i++ {
		serviceURL := cfg.OtherUpstreamServices[i][0]
		serviceEndpoint := cfg.OtherUpstreamServices[i][1]
		upstreamStubClient := upstreamStubSDK.New(serviceURL, serviceEndpoint)
		if upstreamStubClient == nil {
			err := errors.New("failed to create search upstream stub client for upstream service: " + serviceURL + serviceEndpoint)
			log.Error(ctx, err.Error(), err)
			return err
		}
		upstreamServiceClients[i] = upstreamStubClient
	}

	t := &Tracker{}
	errChan := make(chan error, 1)

	docChannels := make([]chan Document, 0)

	if cfg.EnableDatasetAPIReindex {
		datasetChan, _ := extractDatasets(ctx, t, errChan, datasetClient, cfg.ServiceAuthToken, cfg.PaginationLimit)
		editionChan, _ := retrieveDatasetEditions(ctx, t, datasetClient, datasetChan, cfg.ServiceAuthToken, cfg.MaxDatasetExtractions)
		metadataChan, _ := retrieveLatestMetadata(ctx, t, datasetClient, editionChan, cfg.ServiceAuthToken, cfg.MaxDatasetExtractions)
		transformedMetaChan := metaDataTransformer(ctx, t, errChan, metadataChan, cfg.MaxDatasetTransforms)
		docChannels = append(docChannels, transformedMetaChan)
	}

	if cfg.EnableZebedeeReindex {
		topicsMapChan := retrieveTopicsMap(ctx, errChan, cfg.TopicTaggingEnabled, cfg.ServiceAuthToken, topicClient)

		urisChan := uriProducer(ctx, t, errChan, zebClient)
		extractedChan := docExtractor(ctx, t, errChan, zebClient, urisChan, cfg.MaxDocumentExtractions)
		transformedDocChan := docTransformer(ctx, t, errChan, extractedChan, cfg.MaxDocumentTransforms, topicsMapChan)
		docChannels = append(docChannels, transformedDocChan)
	}

	if cfg.EnableOtherServicesReindex {
		for i := 0; i < numUpstreamServices; i++ {
			// Firstly create a channel of Resource items
			resourceChan := getResourceItems(ctx, errChan, upstreamServiceClients[i], cfg.MaxDocumentExtractions)
			log.Info(ctx, "getting resource items", log.Data{"resourceChan": resourceChan})
			// Next transform those Resource items into Document items

			// transResourceChan := resourceTransformer(ctx, t, errChan, resourceChan, cfg.MaxDocumentTransforms)
			//// Then append those Document items to the other Documents in the docChannels
			// docChannels = append(docChannels, transResourceChan)
		}
	}

	joinedChan := joinDocChannels(ctx, t, docChannels...)
	indexedChan := docIndexer(ctx, t, errChan, esClient, joinedChan)

	doneChan := summarize(ctx, indexedChan)

	ticker := time.NewTicker(cfg.TrackerInterval)
	defer ticker.Stop()

	for done := false; !done; {
		select {
		case err := <-errChan:
			log.Error(ctx, "error in reindex", err)
			return err
		case <-doneChan:
			done = true
			break
		case <-ticker.C:
			log.Info(ctx, "tracker summary", log.Data{"counters": t.Get()})
		}
	}

	if err := cleanOldIndices(ctx, esClient); err != nil {
		log.Error(ctx, "failed to clean old indexes", err)
		return err
	}

	return nil
}

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
			if strings.Contains(item.URI, "/timeseries/") && strings.Contains(item.URI, "/previous/") {
				log.Info(ctx, "not indexing uri as previous timeseries", log.Data{
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

// getResourceItems gets the specified maximum number of Resources from the upstream service and then puts each Resource item into a channel, which it returns.
func getResourceItems(ctx context.Context, errChan chan error, upstreamStubClient *upstreamStubSDK.Client, maxExtractions int) (resourcesChan chan upstreamModels.Resource) {
	resourcesChan = make(chan upstreamModels.Resource, defaultChannelBuffer)
	go func() {
		defer close(resourcesChan)

		var opts upstreamStubSDK.Options
		//var limitParam url.Values = make(map[string][]string)
		//limitParam.Add("limit", strconv.Itoa(maxExtractions))
		//opts.Query = limitParam
		// an alternative, if fixed in the upstream stub, would be:
		opts.Limit(strconv.Itoa(maxExtractions))
		resources, err := upstreamStubClient.GetResources(ctx, opts)
		if err != nil {
			errChan <- err
			log.Error(ctx, "failed to get resources from search upstream stub", err, log.Data{"options": opts})
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
		log.Info(ctx, "finished transforming docs")
	}()
	return transformedChan
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

func joinDocChannels(ctx context.Context, tracker *Tracker, inChans ...chan Document) chan Document {
	outChan := make(chan Document, defaultChannelBuffer)
	go func() {
		var wg sync.WaitGroup

		for _, inChan := range inChans {
			wg.Add(1)
			go func(i chan Document) {
				defer wg.Done()
				for d := range i {
					outChan <- d
					tracker.Inc("joined")
				}
			}(inChan)
		}
		wg.Wait()
		close(outChan)

		log.Info(ctx, "finished joining transformed docs channels")
	}()
	return outChan
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

func transformMetadataDoc(ctx context.Context, tracker *Tracker, errChan chan error, metadataChan chan *dataset.Metadata, transformedChan chan<- Document) {
	for m := range metadataChan {
		uri := extractorModels.GetURI(m)

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

		searchDataImport := &extractorModels.SearchDataImport{
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

func docIndexer(ctx context.Context, tracker *Tracker, errorChan chan error, dpEsIndexClient dpEsClient.Client, transformedChan chan Document) chan bool {
	indexedChan := make(chan bool, defaultChannelBuffer)
	go func() {
		defer close(indexedChan)

		indexName := createIndexName("ons")

		log.Info(ctx, "creating index", log.Data{"indexName": indexName})
		err := dpEsIndexClient.CreateIndex(ctx, indexName, elasticsearch.GetSearchIndexSettings())
		if err != nil {
			log.Error(ctx, "error creating index", err)
			errorChan <- err
			return
		}
		log.Info(ctx, "index created", log.Data{"indexName": indexName})

		indexDoc(ctx, tracker, dpEsIndexClient, transformedChan, indexedChan, indexName)

		err = dpEsIndexClient.BulkIndexClose(ctx)
		if err != nil {
			errorChan <- err
		}
		log.Info(ctx, "finished indexing docs")

		err = swapAliases(ctx, dpEsIndexClient, indexName)
		if err != nil {
			errorChan <- err
		}
	}()
	return indexedChan
}

func createIndexName(s string) string {
	now := time.Now()
	return fmt.Sprintf("%s%d", s, now.UnixMicro())
}

// indexDoc reads documents from the transformedChan and calls 'BulkIndexAdd'.
// if the document is added successfully, then 'true' is sent to the indexedChan
// otherwise, 'false' is sent
func indexDoc(ctx context.Context, tracker *Tracker, esClient dpEsClient.Client, transformedChan <-chan Document, indexedChan chan bool, indexName string) {
	for transformedDoc := range transformedChan {
		onSuccess := func(_ context.Context, _ esutil.BulkIndexerItem, _ esutil.BulkIndexerResponseItem) {
			indexedChan <- true
			tracker.Inc("indexed")
		}

		onFailure := func(ctx context.Context, _ esutil.BulkIndexerItem, biri esutil.BulkIndexerResponseItem, err error) {
			log.Error(ctx, "failed to index document", err, log.Data{
				"doc_id":   transformedDoc.ID,
				"response": biri,
			})
			indexedChan <- false
			tracker.Inc("indexed-failure")
		}

		err := esClient.BulkIndexAdd(ctx, v710.Create, indexName, transformedDoc.ID, transformedDoc.Body, onSuccess, onFailure)
		if err != nil {
			log.Error(ctx, "failed to index document", err, log.Data{"doc_id": transformedDoc.ID})
			indexedChan <- false
			tracker.Inc("indexed-error")
		}
	}
}

func swapAliases(ctx context.Context, dpEsIndexClient dpEsClient.Client, indexName string) error {
	updateAliasErr := dpEsIndexClient.UpdateAliases(ctx, "ons", []string{"ons*"}, []string{indexName})
	if updateAliasErr != nil {
		log.Error(ctx, "error swapping aliases: %v", updateAliasErr)
		return updateAliasErr
	}
	return nil
}

func summarize(ctx context.Context, indexedChan <-chan bool) chan bool {
	totalIndexed, totalFailed := 0, 0

	done := make(chan bool, 2)

	go func(dc chan<- bool) {
		for indexed := range indexedChan {
			if indexed {
				totalIndexed++
			} else {
				totalFailed++
			}
		}
		log.Info(ctx, "indexing summary", log.Data{"indexed": totalIndexed, "failed": totalFailed})
		dc <- true
	}(done)

	return done
}

type aliasResponse map[string]indexDetails

type indexDetails struct {
	Aliases map[string]interface{} `json:"aliases"`
}

func cleanOldIndices(ctx context.Context, dpEsIndexClient dpEsClient.Client) error {
	body, err := dpEsIndexClient.GetAlias(ctx) // Create this method via dp-elasticsearch v3 lib
	if err != nil {
		log.Error(ctx, "error getting alias", err)
		return err
	}
	var r aliasResponse
	if err := json.Unmarshal(body, &r); err != nil {
		log.Error(ctx, "error parsing alias response body", err)
		return err
	}

	var toDelete []string
	for index, details := range r {
		if strings.HasPrefix(index, "ons") && !doesIndexHaveAlias(details, "ons") {
			toDelete = append(toDelete, index)
		}
	}

	if len(toDelete) > 0 {
		err := deleteIndicies(ctx, dpEsIndexClient, toDelete)
		if err != nil {
			return err
		}
	}
	return nil
}

func doesIndexHaveAlias(details indexDetails, alias string) bool {
	for k := range details.Aliases {
		if k == alias {
			return true
		}
	}
	return false
}

func deleteIndicies(ctx context.Context, dpEsIndexClient dpEsClient.Client, indicies []string) error {
	if err := dpEsIndexClient.DeleteIndices(ctx, indicies); err != nil {
		log.Error(ctx, "error getting alias", err)
		return err
	}
	log.Info(ctx, "indicies deleted", log.Data{"deleted_indicies": indicies})
	return nil
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
				tracker.Inc("dataset")
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

func convertToSearchDataModel(searchDataImport extractorModels.SearchDataImport) importerModels.SearchDataImport {
	searchDIM := importerModels.SearchDataImport{
		UID:             searchDataImport.UID,
		URI:             searchDataImport.URI,
		DataType:        searchDataImport.DataType,
		Edition:         searchDataImport.Edition,
		JobID:           searchDataImport.JobID,
		SearchIndex:     searchDataImport.SearchIndex,
		CanonicalTopic:  searchDataImport.CanonicalTopic,
		CDID:            searchDataImport.CDID,
		DatasetID:       searchDataImport.DatasetID,
		Keywords:        searchDataImport.Keywords,
		MetaDescription: searchDataImport.MetaDescription,
		ReleaseDate:     searchDataImport.ReleaseDate,
		Summary:         searchDataImport.Summary,
		Title:           searchDataImport.Title,
		Topics:          searchDataImport.Topics,
		TraceID:         searchDataImport.TraceID,
		Cancelled:       searchDataImport.Cancelled,
		Finalised:       searchDataImport.Finalised,
		ProvisionalDate: searchDataImport.ProvisionalDate,
		Published:       searchDataImport.Published,
		Survey:          searchDataImport.Survey,
		Language:        searchDataImport.Language,
	}
	for _, dateChange := range searchDataImport.DateChanges {
		searchDIM.DateChanges = append(searchDIM.DateChanges, importerModels.ReleaseDateDetails{
			ChangeNotice: dateChange.ChangeNotice,
			Date:         dateChange.Date,
		})
	}
	searchDIM.PopulationType = importerModels.PopulationType{
		Key:    searchDataImport.PopulationType.Key,
		AggKey: searchDataImport.PopulationType.AggKey,
		Name:   searchDataImport.PopulationType.Name,
		Label:  searchDataImport.PopulationType.Label,
	}
	for _, dim := range searchDataImport.Dimensions {
		searchDIM.Dimensions = append(searchDIM.Dimensions, importerModels.Dimension{
			Key:      dim.Key,
			AggKey:   dim.AggKey,
			Name:     dim.Name,
			Label:    dim.Label,
			RawLabel: dim.RawLabel,
		})
	}

	return searchDIM
}

func getIDsFromURI(uri string) (datasetID, editionID, versionID string, err error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return "", "", "", err
	}

	s := strings.Split(parsedURL.Path, "/")
	if len(s) < 7 {
		return "", "", "", errors.New("not enough arguments in path for version metadata endpoint")
	}
	datasetID = s[2]
	editionID = s[4]
	versionID = s[6]
	return
}
