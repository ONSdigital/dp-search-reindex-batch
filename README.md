# dp-search-reindex-batch

Batch nomad job for reindexing search

### Getting started

* Run `make debug` to run application (See [Local Prerequisites section](#local-prerequisites) first)
* Run `make help` to see full list of make targets

### Dependencies

* No further dependencies other than those defined in `go.mod`

### Configuration

| Environment variable          | Default                                    | Description                                                                                    |
|-------------------------------|--------------------------------------------|------------------------------------------------------------------------------------------------|
| AWS_REGION                    | "eu-west-2"                                | AWS region                                                                                     |
| AWS_SEC_SKIP_VERIFY           | false                                      | Whether to skip TLS verification for AWS requests                                              |
| DATASET_API_URL               | "http://localhost:22000"                   | URL of the Dataset API                                                                         |
| DATASET_PAGINATION_LIMIT      | 500                                        | Number of datasets to fetch per page of requests to Dataset API                                |
| ENABLE_TOPIC_TAGGING          | false                                      | Whether to enable topic auto-tagging                                                           |
| ELASTIC_SEARCH_URL            | "http://localhost:11200"                   | URL of elastic search server (or AWS Opensearch)                                               |
| MAX_DOCUMENT_EXTRACTIONS      | 100                                        | Max number of concurrent Document Extractions (ie. Zebedee connections)                        |
| MAX_DOCUMENT_TRANSFORMS       | 20                                         | Max number of concurrent Document Transformation workers                                       |
| MAX_DATASET_EXTRACTIONS       | 20                                         | Max number of concurrent Dataset Extractions (ie. Dataset API connections)                     |
| MAX_DATASET_TRANSFORMS        | 10                                         | Max number of concurrent Dataset Transformation workers                                        |
| SERVICE_AUTH_TOKEN            | ""                                         | Zebedee Service Auth Token for API requests                                                    |
| SIGN_ELASTICSEARCH_REQUESTS   | false                                      | Whether to sign elasticsearch requests (true for AWS)                                          |
| TOPIC_API_URL                 | "http://localhost:25300"                   | URL of the Topic API                                                                           |
| TRACKER_INTERVAL              | 5s                                         | Interval for progress tracker summary logging                                                  |
| ZEBEDEE_URL                   | "http://localhost:8082"                    | URL of publishing zebedee                                                                      |
| ZEBEDEE_TIMEOUT               | 2m                                         | Timeout for Zebedee endpoints - published index can take > 2 minutes                           |
| ENABLE_DATASET_API_REINDEX    | false                                      | Whether to get documents from the Dataset API for reindexing or not                            |
| ENABLE_ZEBEDEE_REINDEX        | false                                      | Whether to get documents from Zebedee for reindexing or not                                    |
 | ENABLE_OTHER_SERVICES_REINDEX | false                                      | Whether to get documents from other upstream services orr not                                  |
 | OTHER_UPSTREAM_SERVICES       | {{"http://localhost:29600", "/resources"}} | List of string pairs, each consisting of a domain and endpoint, representing upstream services |

### Local Prerequisites

* Requires ElasticSearch 7.10 running on port 11200
* Requires Zebedee running on port 8082 (and this has a dependency on vault)
* Requires the Dataset API running on port 22000
* Requires the Topic API running on port 25300 if tagging by topic is desired

NB. The Dataset API requires a mongo database named 'datasets', which must contain the following collections:

* contacts
* datasets
* dimension.options
* editions
* instances
* instances_locks

The Dataset API also requires this environment variable to be set to true: DISABLE_GRAPH_DB_DEPENDENCY

Please make sure your elasticsearch server is running locally on localhost:11200 and version of the server is 7.10,
which is the current supported version. You may use `dp-compose/v2/stacks/search` stack for this.

Please ensure to set the 'ENABLE_TOPIC_TAGGING' flag to true, if the topic tagging feature is required.

If you want to run the reindex script locally but loading data from an environment (e.g. `sandbox`), you may
run `dp ssh` with port forwarding for dataset-api and zebedee (please check the services IPs and ports
in `https://consul.dp.aws.onsdigital.uk/ui/eu/services`) For example:

```shell
dp ssh sandbox publishing 2 -p 22000:10.30.138.234:26020
dp ssh sandbox publishing 1 -p 8082:10.30.138.93:25108
```

If you do this the service auth token in the configuration will need to be a valid token accepted in the environment you
are using.

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2023, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

