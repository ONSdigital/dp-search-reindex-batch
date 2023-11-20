package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config represents service configuration for dp-search-reindex-batch
type Config struct {
	ZebedeeURL             string        `envconfig:"ZEBEDEE_URL"`
	ElasticSearchURL       string        `envconfig:"ELASTIC_SEARCH_URL"`
	SignESRequests         bool          `envconfig:"SIGN_ELASTICSEARCH_REQUESTS"`
	AwsRegion              string        `envconfig:"AWS_REGION"`
	AwsSecSkipVerify       bool          `envconfig:"AWS_SEC_SKIP_VERIFY"`
	DatasetAPIURL          string        `envconfig:"DATASET_API_URL"`
	ServiceAuthToken       string        `envconfig:"SERVICE_AUTH_TOKEN"             json:"-"`
	PaginationLimit        int           `envconfig:"DATASET_PAGINATION_LIMIT"`
	MaxDocumentExtractions int           `envconfig:"MAX_DOCUMENT_EXTRACTIONS"`
	MaxDocumentTransforms  int           `envconfig:"MAX_DOCUMENT_TRANSFORMS"`
	MaxDatasetExtractions  int           `envconfig:"MAX_DATASET_EXTRACTIONS"`
	MaxDatasetTransforms   int           `envconfig:"MAX_DATASET_TRANSFORMS"`
	TrackerInterval        time.Duration `envconfig:"TRACKER_INTERVAL"`
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		ZebedeeURL:             "http://localhost:8082",
		ElasticSearchURL:       "http://localhost:11200",
		SignESRequests:         false,
		AwsRegion:              "eu-west-2",
		AwsSecSkipVerify:       false,
		DatasetAPIURL:          "http://localhost:22000",
		ServiceAuthToken:       "",
		PaginationLimit:        500,
		MaxDocumentExtractions: 100,
		MaxDocumentTransforms:  20,
		MaxDatasetExtractions:  20,
		MaxDatasetTransforms:   10,
		TrackerInterval:        5 * time.Second,
	}

	return cfg, envconfig.Process("", cfg)
}
