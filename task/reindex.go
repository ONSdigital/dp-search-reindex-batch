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
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/elastic/go-elasticsearch/v7/esutil"

	"github.com/ONSdigital/dp-search-reindex-batch/config"
)

var (
	maxConcurrentExtractions = 20
	maxConcurrentIndexings   = 30
	DefaultPaginationLimit   = 500
)

type cliConfig struct {
	aws              AWSConfig
	zebedeeURL       string
	datasetURL       string
	esURL            string
	signRequests     bool
	ServiceAuthToken string `json:"-"`
	PaginationLimit  int
	TestSubset       bool // Set this flag to true to request only one batch of datasets from Dataset API
	IgnoreZebedee    bool // Set this flag to true to avoid requesting zebedee datasets
}

type AWSConfig struct {
	filename              string
	region                string
	service               string
	tlsInsecureSkipVerify bool
}

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

func reindex(ctx context.Context) error {
	batchCfg, err := config.Get()
	if err != nil {
		return err // TODO log then wrap err
	}

	// TODO refactor existing code below from old dp-search-api/cmd/reindex/main.go version

	cfg := getConfig(ctx, batchCfg)
	log.Info(ctx, "Running reindex script", log.Data{"name": Name, "config": cfg})

	hcClienter := dphttp2.NewClient()
	if hcClienter == nil {
		err := errors.New("failed to create dp http client")
		log.Fatal(ctx, err.Error(), err)
		panic(err)
	}
	hcClienter.SetMaxRetries(2)
	hcClienter.SetTimeout(2 * time.Minute) // Published Index takes about 10s to return (>1m in sandbox) so add a bit more

	zebClient := zebedee.NewClientWithClienter(cfg.zebedeeURL, hcClienter)
	if !cfg.IgnoreZebedee && zebClient == nil {
		err := errors.New("failed to create zebedee client")
		log.Fatal(ctx, err.Error(), err)
		panic(err)
	}

	esHTTPClient := hcClienter
	if cfg.signRequests {
		log.Info(ctx, "use a signing roundtripper client")
		awsSignerRT, err := awsauth.NewAWSSignerRoundTripper(cfg.aws.filename, cfg.aws.filename, cfg.aws.region, cfg.aws.service,
			awsauth.Options{TlsInsecureSkipVerify: cfg.aws.tlsInsecureSkipVerify})
		if err != nil {
			log.Fatal(ctx, "Failed to create http signer", err)
			panic(err)
		}

		esHTTPClient = dphttp2.NewClientWithTransport(awsSignerRT)
	}

	datasetClient := dataset.NewAPIClient(cfg.datasetURL)
	esClient, esClientErr := dpEs.NewClient(dpEsClient.Config{
		ClientLib: dpEsClient.GoElasticV710,
		Address:   cfg.esURL,
		Transport: esHTTPClient,
	})
	if esClientErr != nil {
		log.Fatal(ctx, "Failed to create dp-elasticsearch client", esClientErr)
		panic(esClientErr)
	}

	if err := esClient.NewBulkIndexer(ctx); err != nil {
		log.Fatal(ctx, "failed to create new bulk indexer", err)
		panic(err)
	}

	t := &Tracker{}

	datasetChan, _ := extractDatasets(ctx, t, datasetClient, cfg)
	editionChan, _ := retrieveDatasetEditions(ctx, t, datasetClient, datasetChan, cfg.ServiceAuthToken)
	metadataChan, _ := retrieveLatestMetadata(ctx, t, datasetClient, editionChan, cfg.ServiceAuthToken)
	transformedMetaChan := metaDataTransformer(ctx, t, metadataChan)

	urisChan := uriProducer(ctx, t, zebClient, cfg)
	extractedChan, extractionFailuresChan := docExtractor(ctx, t, zebClient, urisChan, maxConcurrentExtractions)
	transformedDocChan := docTransformer(ctx, t, extractedChan)

	transformedChan := joinDocChannels(ctx, t, transformedDocChan, transformedMetaChan)
	indexedChan := docIndexer(ctx, t, esClient, transformedChan, maxConcurrentIndexings)

	summarize(ctx, t, indexedChan, extractionFailuresChan)
	cleanOldIndices(ctx, esClient)

	return nil
}

func uriProducer(ctx context.Context, tracker *Tracker, z clients.ZebedeeClient, cfg cliConfig) chan string {
	uriChan := make(chan string, maxConcurrentExtractions)
	go func() {
		defer close(uriChan)
		items := getPublishedURIs(ctx, z, cfg)
		for _, item := range items {
			uriChan <- item.URI
			tracker.Inc("uri")
		}
		log.Info(ctx, "finished listing uris")
	}()
	return uriChan
}

func getPublishedURIs(ctx context.Context, z clients.ZebedeeClient, cfg cliConfig) []zebedee.PublishedIndexItem {
	if cfg.IgnoreZebedee {
		return []zebedee.PublishedIndexItem{}
	}
	index, err := z.GetPublishedIndex(ctx, &zebedee.PublishedIndexRequestParams{})
	if err != nil {
		log.Fatal(ctx, "fatal error getting index from zebedee", err)
		panic(err)
	}
	log.Info(ctx, "fetched uris from zebedee", log.Data{"count": index.Count})
	return index.Items
}

func docExtractor(ctx context.Context, tracker *Tracker, z clients.ZebedeeClient, uriChan chan string, maxExtractions int) (extractedChan chan Document, extractionFailuresChan chan string) {
	extractedChan = make(chan Document, maxExtractions)
	extractionFailuresChan = make(chan string, maxExtractions)
	go func() {
		defer close(extractedChan)
		defer close(extractionFailuresChan)

		var wg sync.WaitGroup

		for w := 0; w < maxExtractions; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				extractDoc(ctx, tracker, z, uriChan, extractedChan, extractionFailuresChan)
			}()
		}
		wg.Wait()
		log.Info(ctx, "finished extracting docs")
	}()
	return
}

func extractDoc(ctx context.Context, tracker *Tracker, z clients.ZebedeeClient, uriChan <-chan string, extractedChan chan Document, extractionFailuresChan chan string) {
	for uri := range uriChan {
		body, err := z.GetPublishedData(ctx, uri)
		if err != nil {
			extractionFailuresChan <- uri
		}

		extractedDoc := Document{
			URI:  uri,
			Body: body,
		}
		extractedChan <- extractedDoc
		tracker.Inc("doc-extracted")
	}
}

func docTransformer(ctx context.Context, tracker *Tracker, extractedChan chan Document) chan Document {
	transformedChan := make(chan Document, maxConcurrentExtractions)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < maxConcurrentExtractions; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				transformZebedeeDoc(ctx, tracker, extractedChan, transformedChan)
				wg.Done()
			}(&wg)
		}
		wg.Wait()
		close(transformedChan)
		log.Info(ctx, "finished transforming docs")
	}()
	return transformedChan
}

func metaDataTransformer(ctx context.Context, tracker *Tracker, metadataChan chan *dataset.Metadata) chan Document {
	transformedChan := make(chan Document, maxConcurrentExtractions)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < maxConcurrentExtractions; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				transformMetadataDoc(ctx, tracker, metadataChan, transformedChan)
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
	outChan := make(chan Document, maxConcurrentExtractions)
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

func transformZebedeeDoc(ctx context.Context, tracker *Tracker, extractedChan chan Document, transformedChan chan<- Document) {
	for extractedDoc := range extractedChan {
		var zebedeeData extractorModels.ZebedeeData
		err := json.Unmarshal(extractedDoc.Body, &zebedeeData)
		if err != nil {
			log.Fatal(ctx, "error while attempting to unmarshal zebedee response into zebedeeData", err) // TODO proper error handling
			panic(err)
		}
		if zebedeeData.Description.Title == "" {
			// Don't want to index things without title
			tracker.Inc("untransformed-notitle")
			continue // move on to the next extracted doc
		}
		exporterEventData := extractorModels.MapZebedeeDataToSearchDataImport(zebedeeData, -1)
		importerEventData := convertToSearchDataModel(exporterEventData)
		esModel := transform.NewTransformer().TransformEventModelToEsModel(&importerEventData)

		body, err := json.Marshal(esModel)
		if err != nil {
			log.Fatal(ctx, "error marshal to json", err) // TODO error handling
			panic(err)
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

func transformMetadataDoc(ctx context.Context, tracker *Tracker, metadataChan chan *dataset.Metadata, transformedChan chan<- Document) {
	for m := range metadataChan {
		uri := extractorModels.GetURI(m)

		parsedURI, err := url.Parse(uri)
		if err != nil {
			log.Fatal(ctx, "error occurred while parsing url", err)
			panic(err)
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
			log.Fatal(ctx, "error occurred while mapping dataset metadata values", err)
			panic(err)
		}

		importerEventData := convertToSearchDataModel(*searchDataImport)
		esModel := transform.NewTransformer().TransformEventModelToEsModel(&importerEventData)
		body, err := json.Marshal(esModel)
		if err != nil {
			log.Fatal(ctx, "error marshal to json", err) // TODO error handling
			panic(err)
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

func docIndexer(ctx context.Context, tracker *Tracker, dpEsIndexClient dpEsClient.Client, transformedChan chan Document, maxIndexings int) chan bool {
	indexedChan := make(chan bool, maxIndexings)
	go func() {
		defer close(indexedChan)

		indexName := createIndexName("ons")

		log.Info(ctx, "creating index", log.Data{"indexName": indexName})
		err := dpEsIndexClient.CreateIndex(ctx, indexName, elasticsearch.GetSearchIndexSettings())
		if err != nil {
			log.Fatal(ctx, "error creating index", err)
			panic(err)
		}
		log.Info(ctx, "index created", log.Data{"indexName": indexName})

		indexDoc(ctx, tracker, dpEsIndexClient, transformedChan, indexedChan, indexName)

		dpEsIndexClient.BulkIndexClose(ctx)
		log.Info(ctx, "finished indexing docs")

		swapAliases(ctx, dpEsIndexClient, indexName)
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
		onSuccess := func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			indexedChan <- true
			tracker.Inc("indexed")
		}

		onFailure := func(ctx context.Context, bii esutil.BulkIndexerItem, biri esutil.BulkIndexerResponseItem, err error) {
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

func swapAliases(ctx context.Context, dpEsIndexClient dpEsClient.Client, indexName string) {
	updateAliasErr := dpEsIndexClient.UpdateAliases(ctx, "ons", []string{"ons*"}, []string{indexName})
	if updateAliasErr != nil {
		log.Fatal(ctx, "error swapping aliases: %v", updateAliasErr)
		panic(updateAliasErr)
	}
}

func summarize(ctx context.Context, tracker *Tracker, indexedChan <-chan bool, extractionFailuresChan <-chan string) {
	totalIndexed, totalFailed := 0, 0

	done := make(chan bool, 2)

	// Summarize the extraction failures
	// TODO replace when refactoring of error handling is done
	go func(dc chan<- bool) {
		for range extractionFailuresChan {
			totalFailed++
		}
		dc <- true
	}(done)

	go func(dc chan<- bool) {
		for indexed := range indexedChan {
			if indexed {
				totalIndexed++
			} else {
				totalFailed++
			}
		}
		dc <- true
	}(done)

	totalDone := 0
	for totalDone < 2 {
		log.Info(ctx, "tracker summary", log.Data{"counters": tracker.Get()})
		select {
		case <-done:
			totalDone++
		default:
			time.Sleep(5 * time.Second) // TODO make configurable (when refactoring of config takes place)
		}
	}

	log.Info(ctx, "indexing summary", log.Data{"indexed": totalIndexed, "failed": totalFailed})
}

type aliasResponse map[string]indexDetails

type indexDetails struct {
	Aliases map[string]interface{} `json:"aliases"`
}

func cleanOldIndices(ctx context.Context, dpEsIndexClient dpEsClient.Client) {
	body, err := dpEsIndexClient.GetAlias(ctx) // Create this method via dp-elasticsearch v3 lib
	if err != nil {
		log.Fatal(ctx, "error getting alias", err)
		panic(err)
	}
	var r aliasResponse
	if err := json.Unmarshal(body, &r); err != nil {
		log.Fatal(ctx, "error parsing alias response body", err)
		panic(err)
	}

	toDelete := []string{}
	for index, details := range r {
		if strings.HasPrefix(index, "ons") && !doesIndexHaveAlias(details, "ons") {
			toDelete = append(toDelete, index)
		}
	}

	if len(toDelete) > 0 {
		deleteIndicies(ctx, dpEsIndexClient, toDelete)
	}
}

func doesIndexHaveAlias(details indexDetails, alias string) bool {
	for k := range details.Aliases {
		if k == alias {
			return true
		}
	}
	return false
}

func deleteIndicies(ctx context.Context, dpEsIndexClient dpEsClient.Client, indicies []string) {
	if err := dpEsIndexClient.DeleteIndices(ctx, indicies); err != nil {
		log.Fatal(ctx, "error getting alias", err)
		panic(err)
	}
	log.Info(ctx, "indicies deleted", log.Data{"deleted_indicies": indicies})
}

func extractDatasets(ctx context.Context, tracker *Tracker, datasetClient clients.DatasetAPIClient, cfg cliConfig) (chan dataset.Dataset, *sync.WaitGroup) {
	datasetChan := make(chan dataset.Dataset, maxConcurrentExtractions)
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
			list, err = datasetClient.GetDatasets(ctx, "", cfg.ServiceAuthToken, "", &dataset.QueryParams{
				Offset: offset,
				Limit:  cfg.PaginationLimit,
			})
			if err != nil {
				log.Fatal(ctx, "error retrieving datasets", err)
				panic(err)
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
			offset += cfg.PaginationLimit

			if offset > list.TotalCount {
				break
			}
		}
	}

	// extractSome extracts only one batch of size 'PaginationLimit' from datasetAPI
	extractSome := func() {
		defer func() {
			close(datasetChan)
			wg.Done()
		}()
		var list dataset.List
		var err error
		var offset = 0
		list, err = datasetClient.GetDatasets(ctx, "", cfg.ServiceAuthToken, "", &dataset.QueryParams{
			Offset: offset,
			Limit:  cfg.PaginationLimit,
		})
		if err != nil {
			log.Fatal(ctx, "error retrieving datasets", err)
			panic(err)
		}
		for i := 0; i < len(list.Items); i++ {
			datasetChan <- list.Items[i]
		}
	}

	wg.Add(1)
	if cfg.TestSubset {
		go extractSome()
	} else {
		go extractAll()
	}

	return datasetChan, &wg
}

func retrieveDatasetEditions(ctx context.Context, tracker *Tracker, datasetClient clients.DatasetAPIClient, datasetChan chan dataset.Dataset, serviceAuthToken string) (chan DatasetEditionMetadata, *sync.WaitGroup) {
	editionMetadataChan := make(chan DatasetEditionMetadata, maxConcurrentExtractions)
	var wg sync.WaitGroup
	go func() {
		defer close(editionMetadataChan)
		for i := 0; i < maxConcurrentExtractions; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for dataset := range datasetChan {
					if dataset.Current == nil {
						continue
					}
					editions, err := datasetClient.GetFullEditionsDetails(ctx, "", serviceAuthToken, dataset.CollectionID, dataset.Current.ID)
					if err != nil {
						log.Warn(ctx, "error retrieving editions", log.Data{
							"err":           err,
							"dataset_id":    dataset.Current.ID,
							"collection_id": dataset.CollectionID,
						})
						continue
					}
					for i := 0; i < len(editions); i++ {
						if editions[i].ID == "" || editions[i].Current.Links.LatestVersion.ID == "" {
							continue
						}
						editionMetadataChan <- DatasetEditionMetadata{
							id:        dataset.Current.ID,
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

func retrieveLatestMetadata(ctx context.Context, tracker *Tracker, datasetClient clients.DatasetAPIClient, editionMetadata chan DatasetEditionMetadata, serviceAuthToken string) (chan *dataset.Metadata, *sync.WaitGroup) {
	metadataChan := make(chan *dataset.Metadata, maxConcurrentExtractions)
	var wg sync.WaitGroup
	go func() {
		defer close(metadataChan)
		for i := 0; i < maxConcurrentExtractions; i++ {
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
