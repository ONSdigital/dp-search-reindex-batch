package task

import (
	"context"
	"fmt"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/zebedee"
	"github.com/ONSdigital/dp-search-api/clients/mock"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/smartystreets/goconvey/convey"
)

func TestURIProducer(t *testing.T) {
	convey.Convey("Given a zebedeeClient that returns results", t, func() {
		tracker := &Tracker{}
		errChan := make(chan error, 1)

		zebClient := &mock.ZebedeeClientMock{
			GetPublishedIndexFunc: func(ctx context.Context, piRequest *zebedee.PublishedIndexRequestParams) (zebedee.PublishedIndex, error) {
				return zebedee.PublishedIndex{
					Count: 4,
					Items: []zebedee.PublishedIndexItem{
						{URI: "/economy"},
						{URI: "/timeseries/ec12/previous/nov2011"},
						{URI: "/timeseries/ec12"},
						{URI: "/dataset/ec12/previous"},
					},
					Limit:      20,
					Offset:     0,
					TotalCount: 4,
				}, nil
			},
		}

		convey.Convey("When the URI Producer is called", func() {
			uriChan := uriProducer(ctx, tracker, errChan, zebClient)

			convey.Convey("Then the expected uris are sent to the uris channel", func() {
				uris := make([]string, 3)
				for uri := range uriChan {
					fmt.Println(uri)
					uris = append(uris, uri)
				}

				convey.So(uris, convey.ShouldContain, "/economy")
				convey.So(uris, convey.ShouldContain, "/timeseries/ec12")
				convey.So(uris, convey.ShouldContain, "/dataset/ec12/previous")
				convey.So(uris, convey.ShouldNotContain, "/timeseries/ec12/previous/nov2011")
			})
		})
	})
}

func TestTagImportDataTopics(t *testing.T) {
	convey.Convey("Given a valid SearchDataImport while transforming zebedeeDoc ", t, func() {
		topicsMap := map[string]Topic{
			"economy":               {ID: "6734", Slug: "economy", ParentSlug: ""},
			"environmentalaccounts": {ID: "1834", Slug: "environmentalaccounts", ParentSlug: "economy"},
			"business":              {ID: "1234", Slug: "business", ParentSlug: ""},
		}

		importerEventData := models.SearchDataImport{
			UID:             "12345",
			URI:             "/economy/environmentalaccounts",
			DataType:        "dataset",
			Edition:         "2021",
			JobID:           "job-123",
			SearchIndex:     "search-index",
			CanonicalTopic:  "canonical-topic",
			CDID:            "CDID-123",
			DatasetID:       "dataset-123",
			Keywords:        []string{"keyword1", "keyword2"},
			MetaDescription: "meta description",
			ReleaseDate:     "2021-01-01",
			Summary:         "summary",
			Title:           "title",
			Topics:          []string{"1234"},
			TraceID:         "trace-123",
			Cancelled:       false,
			Finalised:       false,
			ProvisionalDate: "2021-01-02",
			Published:       true,
			Survey:          "survey",
			Language:        "en",
		}
		convey.Convey("When topics are initially empty", func() {
			importerEventData.Topics = []string{}
			updatedImporterEventData := tagImportDataTopics(topicsMap, importerEventData)

			convey.Convey("Then the resulting topics should contain only topics from the URI", func() {
				expectedTopics := []string{"6734", "1834"}

				convey.So(updatedImporterEventData.Topics, convey.ShouldHaveLength, len(expectedTopics))
				convey.So(updatedImporterEventData.Topics, convey.ShouldContain, "1834")
				convey.So(updatedImporterEventData.Topics, convey.ShouldContain, "6734")
			})
		})

		convey.Convey("When topics are initially not empty", func() {
			updatedImporterEventData := tagImportDataTopics(topicsMap, importerEventData)

			convey.Convey("Then the resulting topics should contain both the default and the topics obtained from the URI", func() {
				expectedTopics := []string{"1234", "6734", "1834"}

				convey.So(updatedImporterEventData.Topics, convey.ShouldHaveLength, len(expectedTopics))
				convey.So(updatedImporterEventData.Topics, convey.ShouldContain, "1834")
				convey.So(updatedImporterEventData.Topics, convey.ShouldContain, "1234")
			})
		})

		convey.Convey("When URI does not match any topics", func() {
			importerEventData.URI = "/non-existing-topic"
			updatedImporterEventData := tagImportDataTopics(topicsMap, importerEventData)

			convey.Convey("Then the resulting topics should contain only the default", func() {
				convey.So(updatedImporterEventData.Topics, convey.ShouldResemble, importerEventData.Topics)
				convey.So(updatedImporterEventData.Topics, convey.ShouldHaveLength, len(importerEventData.Topics))
			})
		})

		convey.Convey("When URI segments contain unrelated topics", func() {
			importerEventData.URI = "/economy/environmentalaccounts/non-existing-slug"
			updatedImporterEventData := tagImportDataTopics(topicsMap, importerEventData)

			convey.Convey("Then the resulting topics should contain only related topics", func() {
				expectedTopics := []string{"1234", "6734", "1834"}

				convey.So(updatedImporterEventData.Topics, convey.ShouldHaveLength, len(expectedTopics))
				convey.So(updatedImporterEventData.Topics, convey.ShouldContain, "1234")
				convey.So(updatedImporterEventData.Topics, convey.ShouldContain, "6734")
				convey.So(updatedImporterEventData.Topics, convey.ShouldContain, "1834")
			})
		})
	})
}

func TestAddTopicWithParents(t *testing.T) {
	convey.Convey("Given a set of topics with duplicate slugs and parent relationships", t, func() {
		topicMap := map[string]Topic{
			"4116": {ID: "4116", Slug: "news", ParentSlug: ""},
			"3543": {ID: "3543", Slug: "news", ParentSlug: "news"},
			"1249": {ID: "1249", Slug: "statementsandletters", ParentSlug: "news"},
		}

		convey.Convey("When adding the root topic with slug 'news'", func() {
			uniqueTopics := make(map[string]struct{})
			AddTopicWithParents("news", "", topicMap, uniqueTopics)

			convey.Convey("It should add the root topic only", func() {
				convey.So(uniqueTopics, convey.ShouldContainKey, "4116")
				convey.So(uniqueTopics, convey.ShouldHaveLength, 1)
			})
		})

		convey.Convey("When adding the subtopic with slug 'news' which has a parent 'news", func() {
			uniqueTopics := make(map[string]struct{})
			AddTopicWithParents("news", "news", topicMap, uniqueTopics)

			convey.Convey("It should add both the parent and child topics", func() {
				convey.So(uniqueTopics, convey.ShouldContainKey, "4116")
				convey.So(uniqueTopics, convey.ShouldContainKey, "3543")
				convey.So(uniqueTopics, convey.ShouldHaveLength, 2)
			})
		})

		convey.Convey("When adding the subtopic with slug 'statementsandletters' which has a parent 'news'", func() {
			uniqueTopics := make(map[string]struct{})
			AddTopicWithParents("statementsandletters", "news", topicMap, uniqueTopics)

			convey.Convey("It should add the statementsandletters topic and its parent 'news'", func() {
				convey.So(uniqueTopics, convey.ShouldContainKey, "4116")
				convey.So(uniqueTopics, convey.ShouldContainKey, "1249")
				convey.So(uniqueTopics, convey.ShouldHaveLength, 2)
			})
		})
	})
}

func TestMigratedContentFiltering(t *testing.T) {
	convey.Convey("Given a extractedChan channel and a transformedChan channel", t, func() {
		errChan := make(chan error, 1)
		extractedChan := make(chan Document, 1)
		transformedChan := make(chan Document, 1)
		tracker := &Tracker{}

		convey.Convey("When a non-editorial document with a migration link is sent to the extractedChan and consumed by transformZebedeeDoc", func() {
			extractedChan <- Document{
				ID:  "doc1",
				URI: "economy/migrated-content",
				Body: []byte(`{
					"type": "static_landing_page",
					"description": {
						"title": "A static landing page title",
						"migrationLink": "some-new-location"
					}
				}`),
			}
			close(extractedChan)

			transformZebedeeDoc(ctx, tracker, errChan, extractedChan, transformedChan, nil)

			convey.Convey("Then the document is filtered out and not sent to the transformed channel", func() {
				convey.So(transformedChan, convey.ShouldBeEmpty)
			})
		})

		convey.Convey("When a non-editorial document without a migration link is sent to the extractedChan and consumed by transformZebedeeDoc", func() {
			extractedChan <- Document{
				ID:  "doc2",
				URI: "economy/non-migrated-content",
				Body: []byte(`{
				"type": "static_landing_page",
				"description": {
					"title": "A static landing page title"
				}
			}`),
			}
			close(extractedChan)

			transformZebedeeDoc(ctx, tracker, errChan, extractedChan, transformedChan, nil)

			convey.Convey("Then the document is sent to the transformed channel", func() {
				convey.So(transformedChan, convey.ShouldHaveLength, 1)
				transformed := <-transformedChan
				convey.So(transformed.URI, convey.ShouldEqual, "economy/non-migrated-content")
			})
		})

		convey.Convey("When an editorial document with a migration link is sent to the extractedChan and consumed by transformZebedeeDoc", func() {
			extractedChan <- Document{
				ID:  "doc3",
				URI: "bulletin/migrated-editorial-content",
				Body: []byte(`{
				"type": "bulletin",
				"description": {
					"title": "A bulletin title",
					"migrationLink": "some-new-location"
				}
			}`),
			}
			close(extractedChan)

			transformZebedeeDoc(ctx, tracker, errChan, extractedChan, transformedChan, nil)

			convey.Convey("Then the document is sent to the transformed channel", func() {
				convey.So(transformedChan, convey.ShouldHaveLength, 1)
				transformed := <-transformedChan
				convey.So(transformed.URI, convey.ShouldEqual, "bulletin/migrated-editorial-content")
			})
		})
	})
}
