package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	upstreamStubSDK "github.com/ONSdigital/dis-search-upstream-stub/sdk"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/zebedee"
	dpEs "github.com/ONSdigital/dp-elasticsearch/v3"
	dpEsClient "github.com/ONSdigital/dp-elasticsearch/v3/client"
	v710 "github.com/ONSdigital/dp-elasticsearch/v3/client/elasticsearch/v710"
	"github.com/ONSdigital/dp-net/v3/awsauth"
	dphttp2 "github.com/ONSdigital/dp-net/v3/http"
	"github.com/ONSdigital/dp-search-api/elasticsearch"
	extractorModels "github.com/ONSdigital/dp-search-data-extractor/models"
	importerModels "github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-reindex-batch/config"
	topicCli "github.com/ONSdigital/dp-topic-api/sdk"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

const (
	defaultChannelBuffer = 20
	awsESService         = "es"
)

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
		awsSignerRT, err := awsauth.NewAWSSignerRoundTripper(ctx, "", "", cfg.AwsRegion, awsESService,
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

	for i, upstreamService := range cfg.OtherUpstreamServices {
		upstreamStubClient := upstreamStubSDK.New(upstreamService.Host, upstreamService.Endpoint)
		if upstreamStubClient == nil {
			err := errors.New("failed to create client for upstream service: " + upstreamService.Host + upstreamService.Endpoint)
			log.Error(ctx, err.Error(), err)
			return err
		}
		upstreamServiceClients[i] = upstreamStubClient
	}

	t := &Tracker{}
	errChan := make(chan error, 1)

	docChannels := make([]chan Document, 0)

	// DEVELOPER WARNING, the following code is concurrent not sequential, each function is running its functionality
	// in its own go routine which sends results down a channel some time in the future as and when it has retrieved or
	// processed them. The functions themselves return immediately as soon as the routines are initiated and return not
	// the final results but rather a channel which will at some point have results flowing through them.
	// Do not develop code here expecting the functionality to happen in sequence, or you will have unexpected results
	// Note the code therefore pretty much immediately runs to the "End of main concurrent section" comment below.

	if cfg.EnableDatasetAPIReindex {
		datasetChan, _ := extractDatasets(ctx, t, errChan, datasetClient, cfg.ServiceAuthToken, cfg.DatasetPaginationLimit)
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
			resourceChan := resourceGetter(ctx, t, errChan, upstreamServiceClients[i], cfg.UpstreamPaginationLimit, cfg.MaxUpstreamExtractions)
			transformedResChan := resourceTransformer(ctx, t, errChan, resourceChan, cfg.MaxUpstreamTransforms)
			docChannels = append(docChannels, transformedResChan)
		}
	}

	joinedChan := joinDocChannels(ctx, t, docChannels...)
	indexedChan := docIndexer(ctx, t, errChan, esClient, joinedChan)

	doneChan := summarize(ctx, indexedChan)

	ticker := time.NewTicker(cfg.TrackerInterval)
	defer ticker.Stop()

	// End of main concurrent section

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
	log.Info(ctx, "swapped aliases", log.Data{"index_name": indexName})
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
