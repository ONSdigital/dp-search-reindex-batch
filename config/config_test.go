package config

import (
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig(t *testing.T) {
	os.Clearenv()
	var err error
	var configuration *Config

	Convey("Given an environment with no environment variables set", t, func() {
		Convey("Then cfg should be nil", func() {
			So(cfg, ShouldBeNil)
		})

		Convey("When the config values are retrieved", func() {
			Convey("Then there should be no error returned, and values are as expected", func() {
				configuration, err = Get() // This Get() is only called once, when inside this function
				So(err, ShouldBeNil)
				So(configuration, ShouldResemble, &Config{
					ZebedeeURL:              "http://localhost:8082",
					ElasticSearchURL:        "http://localhost:11200",
					SignESRequests:          false,
					AwsRegion:               "eu-west-2",
					AwsSecSkipVerify:        false,
					DatasetAPIURL:           "http://localhost:22000",
					ServiceAuthToken:        "",
					PaginationLimit:         500,
					MaxDocumentExtractions:  100,
					MaxDocumentTransforms:   20,
					MaxDatasetExtractions:   20,
					MaxDatasetTransforms:    10,
					TrackerInterval:         5000 * time.Millisecond,
					TopicAPIURL:             "http://localhost:25300",
					TopicTaggingEnabled:     false,
					ZebedeeTimeout:          3 * time.Minute,
					EnableDatasetAPIReindex: false,
					EnableZebedeeReindex:    false,
					OtherUpstreamServices: UpStreamServices{
						UpStreamService{
							Host:     "http://localhost:29600",
							Endpoint: "/resources",
						},
					},
				},
				)
			})

			Convey("Then a second call to config should return the same config", func() {
				// This achieves code coverage of the first return in the Get() function.
				newCfg, newErr := Get()
				So(newErr, ShouldBeNil)
				So(newCfg, ShouldResemble, cfg)
			})
		})
	})
}
