# dp-search-reindex-batch

Batch nomad job for reindexing search

### Getting started

* Run `make debug` to run application
* Run * Run `make help` to see full list of make targets

### Dependencies

* No further dependencies other than those defined in `go.mod`

### Configuration

| Environment variable        | Default                  | Description                                                                |
|-----------------------------|--------------------------|----------------------------------------------------------------------------|
| ZEBEDEE_URL                 | "http://localhost:8082"  | URL of publishing zebedee                                                  |
| ELASTIC_SEARCH_URL          | "http://localhost:11200" | URL of elastic search server (or AWS Opensearch)                           |
| SIGN_ELASTICSEARCH_REQUESTS | false                    | Whether to sign elasticsearch requests (true for AWS)                      |
| AWS_REGION                  | "eu-west-2"              | AWS region                                                                 |
| AWS_SEC_SKIP_VERIFY         | false                    | Whether to skip TLS verification for AWS requests                          |
| DATASET_API_URL             | "http://localhost:22000" | URL of the Dataset API                                                     |
| SERVICE_AUTH_TOKEN          | ""                       | Zebedee Service Auth Token for API requests                                |
| DATASET_PAGINATION_LIMIT    | 500                      | Number of datasets to fetch per page of requests to Dataset API            |
| MAX_DOCUMENT_EXTRACTIONS    | 100                      | Max number of concurrent Document Extractions (ie. Zebedee connections)    |
| MAX_DOCUMENT_TRANSFORMS     | 20                       | Max number of concurrent Document Transformation workers                   |
| MAX_DATASET_EXTRACTIONS     | 20                       | Max number of concurrent Dataset Extractions (ie. Dataset API connections) |
| MAX_DATASET_TRANSFORMS      | 10                       | Max number of concurrent Dataset Transformation workers                    |

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2023, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

