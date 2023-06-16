package task

import (
	"context"

	"github.com/ONSdigital/dp-search-reindex-batch/config"
)

var Name = "aws"

// TODO temporary config adapter to make current reindex script work with batch config

func getConfig(ctx context.Context, cfg *config.Config) cliConfig {
	return cliConfig{
		aws: AWSConfig{
			region:                cfg.AwsRegion,
			service:               "es",
			tlsInsecureSkipVerify: cfg.AwsSecSkipVerify,
		},
		zebedeeURL:       cfg.ZebedeeURL,
		esURL:            cfg.ElasticSearchURL,
		signRequests:     cfg.SignESRequests,
		datasetURL:       cfg.DatasetAPIURL,
		ServiceAuthToken: cfg.ServiceAuthToken,
		PaginationLimit:  cfg.PaginationLimit,
		TestSubset:       false,
		IgnoreZebedee:    false,
	}
}
