package config

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type UpStreamServices []UpStreamService

type UpStreamService struct {
	Host     string
	Endpoint string
}

func (u *UpStreamServices) UnmarshalText(text []byte) error {
	var services [][]string

	err := json.Unmarshal(text, &services)
	if err != nil {
		return err
	}

	*u = nil

	for _, serviceData := range services {
		if len(serviceData) == 2 {
			service := UpStreamService{
				Host:     serviceData[0],
				Endpoint: serviceData[1],
			}

			*u = append(*u, service)
		} else {
			return fmt.Errorf("service data provided did not match expected")
		}
	}

	return nil
}

// Config represents service configuration for dp-search-reindex-batch
type Config struct {
	AwsRegion                  string           `envconfig:"AWS_REGION"`
	AwsSecSkipVerify           bool             `envconfig:"AWS_SEC_SKIP_VERIFY"`
	DatasetAPIURL              string           `envconfig:"DATASET_API_URL"`
	ElasticSearchURL           string           `envconfig:"ELASTIC_SEARCH_URL"`
	EnableDatasetAPIReindex    bool             `envconfig:"ENABLE_DATASET_API_REINDEX"`
	EnableOtherServicesReindex bool             `envconfig:"ENABLE_OTHER_SERVICES_REINDEX"`
	EnableZebedeeReindex       bool             `envconfig:"ENABLE_ZEBEDEE_REINDEX"`
	MaxDatasetExtractions      int              `envconfig:"MAX_DATASET_EXTRACTIONS"`
	MaxDatasetTransforms       int              `envconfig:"MAX_DATASET_TRANSFORMS"`
	MaxDocumentExtractions     int              `envconfig:"MAX_DOCUMENT_EXTRACTIONS"`
	MaxDocumentTransforms      int              `envconfig:"MAX_DOCUMENT_TRANSFORMS"`
	OtherUpstreamServices      UpStreamServices `envconfig:"OTHER_UPSTREAM_SERVICES"`
	PaginationLimit            int              `envconfig:"DATASET_PAGINATION_LIMIT"`
	ServiceAuthToken           string           `envconfig:"SERVICE_AUTH_TOKEN"             json:"-"`
	SignESRequests             bool             `envconfig:"SIGN_ELASTICSEARCH_REQUESTS"`
	TopicAPIURL                string           `envconfig:"TOPIC_API_URL"`
	TopicTaggingEnabled        bool             `envconfig:"ENABLE_TOPIC_TAGGING"`
	TrackerInterval            time.Duration    `envconfig:"TRACKER_INTERVAL"`
	ZebedeeTimeout             time.Duration    `envconfig:"ZEBEDEE_TIMEOUT"`
	ZebedeeURL                 string           `envconfig:"ZEBEDEE_URL"`
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		AwsRegion:                  "eu-west-2",
		AwsSecSkipVerify:           false,
		DatasetAPIURL:              "http://localhost:22000",
		ElasticSearchURL:           "http://localhost:11200",
		EnableDatasetAPIReindex:    false,
		EnableOtherServicesReindex: false,
		EnableZebedeeReindex:       false,
		MaxDatasetExtractions:      20,
		MaxDatasetTransforms:       10,
		MaxDocumentExtractions:     100,
		MaxDocumentTransforms:      20,
		OtherUpstreamServices: UpStreamServices{
			UpStreamService{
				Host:     "http://localhost:29600",
				Endpoint: "/resources",
			},
		},
		PaginationLimit:     500,
		ServiceAuthToken:    "",
		SignESRequests:      false,
		TopicAPIURL:         "http://localhost:25300",
		TopicTaggingEnabled: false,
		TrackerInterval:     5 * time.Second,
		ZebedeeTimeout:      3 * time.Minute,
		ZebedeeURL:          "http://localhost:8082",
	}

	return cfg, envconfig.Process("", cfg)
}
