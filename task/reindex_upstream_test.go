package task

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/ONSdigital/dis-search-upstream-stub/models"
	models2 "github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/smartystreets/goconvey/convey"
)

func TestTransformResourceItem(t *testing.T) {
	convey.Convey("Given a resource channel and a transformed document channel and a topics map", t, func() {
		resourceChan := make(chan models.Resource, 1)
		transformedResChan := make(chan Document, 1)
		tracker := &Tracker{}
		errChan := make(chan error, 1)

		topicsMap := make(map[string]Topic)
		topicsMap["a"] = Topic{ID: "id",
			Slug:       "economy",
			ParentID:   "",
			ParentSlug: ""}

		convey.Convey("When a release resource item is sent to the channel and consumed by transformResourceItem", func() {
			sent := models.Resource{
				URI:             "/a/uri",
				URIOld:          "/an/old/uri",
				ContentType:     "release",
				CDID:            "A321B",
				DatasetID:       "ASELECTIONOFNUMBERSANDLETTERS456",
				Edition:         "an edition",
				MetaDescription: "a description",
				ReleaseDate:     "2024-11-21:20:14Z",
				Summary:         "a summary",
				Title:           "a title",
				Language:        "string",
				Survey:          "string",
				CanonicalTopic:  "string",
				Cancelled:       true,
				Finalised:       true,
				Published:       true,
				ProvisionalDate: "October-November 2024",
				DateChanges: []models.ReleaseDateDetails{
					{
						ChangeNotice: "a change_notice",
						PreviousDate: "2023-11-21:20:14Z",
					},
				},
			}

			expected := &models2.EsModel{
				URI:             "/a/uri",
				Edition:         "an edition",
				DataType:        "release",
				CDID:            "A321B",
				DatasetID:       "ASELECTIONOFNUMBERSANDLETTERS456",
				MetaDescription: "a description",
				ReleaseDate:     "2024-11-21:20:14Z",
				Summary:         "a summary",
				Title:           "a title",
				Topics:          []string{},
				Language:        "string",
				Survey:          "string",
				CanonicalTopic:  "string",
				Cancelled:       true,
				Finalised:       true,
				Published:       true,
				ProvisionalDate: "October-November 2024",
				DateChanges: []models2.ReleaseDateChange{
					{
						ChangeNotice: "a change_notice",
						Date:         "2023-11-21:20:14Z",
					},
				},
				PopulationType: &models2.EsPopulationType{Key: "", AggKey: "", Name: "", Label: ""},
				Dimensions:     []models2.EsDimension(nil),
			}

			resourceChan <- sent
			close(resourceChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func(waitGroup *sync.WaitGroup) {
				transformResourceItem(ctx, tracker, errChan, resourceChan, transformedResChan, topicsMap)
				wg.Done()
			}(wg)

			convey.Convey("Then the expected elasticsearch document is sent to the transformed channel", func() {
				transformed := <-transformedResChan
				convey.So(transformed.ID, convey.ShouldEqual, "/a/uri")
				convey.So(transformed.URI, convey.ShouldEqual, "/a/uri")

				esModel := &models2.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				convey.So(err, convey.ShouldBeNil)
				convey.So(esModel, convey.ShouldResemble, expected)

				wg.Wait()
			})
		})
		convey.Convey("When a non-release resource item is sent to the channel and consumed by transformResourceItem", func() {
			sent := models.Resource{
				URI:             "/a/uri",
				URIOld:          "/an/old/uri",
				ContentType:     "standard",
				CDID:            "A321B",
				DatasetID:       "ASELECTIONOFNUMBERSANDLETTERS456",
				Edition:         "an edition",
				MetaDescription: "a description",
				ReleaseDate:     "2024-11-21:20:14Z",
				Summary:         "a summary",
				Title:           "a title",
				Language:        "string",
				Survey:          "string",
				CanonicalTopic:  "string",
			}

			expected := &models2.EsModel{
				URI:             "/a/uri",
				Edition:         "an edition",
				DataType:        "standard",
				CDID:            "A321B",
				DatasetID:       "ASELECTIONOFNUMBERSANDLETTERS456",
				MetaDescription: "a description",
				ReleaseDate:     "2024-11-21:20:14Z",
				Summary:         "a summary",
				Title:           "a title",
				Topics:          []string{},
				Language:        "string",
				Survey:          "string",
				CanonicalTopic:  "string",
				PopulationType:  &models2.EsPopulationType{Key: "", AggKey: "", Name: "", Label: ""},
			}

			resourceChan <- sent
			close(resourceChan)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func(waitGroup *sync.WaitGroup) {
				transformResourceItem(ctx, tracker, errChan, resourceChan, transformedResChan, topicsMap)
				wg.Done()
			}(wg)

			convey.Convey("Then the expected elasticsearch document is sent to the transformed channel", func() {
				transformed := <-transformedResChan
				convey.So(transformed.ID, convey.ShouldEqual, "/a/uri")
				convey.So(transformed.URI, convey.ShouldEqual, "/a/uri")

				esModel := &models2.EsModel{}
				err := json.Unmarshal(transformed.Body, esModel)
				convey.So(err, convey.ShouldBeNil)
				convey.So(esModel, convey.ShouldResemble, expected)

				wg.Wait()
			})
		})
	})
}
