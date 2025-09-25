package task

import (
	"context"
	"errors"
	"testing"

	"github.com/ONSdigital/dp-topic-api/models"
	"github.com/ONSdigital/dp-topic-api/sdk"
	topicCliErr "github.com/ONSdigital/dp-topic-api/sdk/errors"
	mockTopic "github.com/ONSdigital/dp-topic-api/sdk/mocks"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testEconomyRootTopic = models.Topic{
		ID:          "6734",
		Title:       "Economy",
		SubtopicIds: &[]string{"1834"},
	}

	testEconomyRootTopicPrivate = models.TopicResponse{
		ID:      "6734",
		Next:    &testEconomyRootTopic,
		Current: &testEconomyRootTopic,
	}

	testBusinessRootTopic = models.Topic{
		ID:          "1234",
		Title:       "Business",
		SubtopicIds: &[]string{},
	}

	testBusinessRootTopicPrivate = models.TopicResponse{
		ID:      "1234",
		Next:    &testBusinessRootTopic,
		Current: &testBusinessRootTopic,
	}
)

func TestLoadTopicsMap(t *testing.T) {
	ctx := context.Background()
	serviceAuthToken := "test-token"

	mockClient := &mockTopic.ClienterMock{
		GetRootTopicsPrivateFunc: func(ctx context.Context, reqHeaders sdk.Headers) (*models.PrivateSubtopics, topicCliErr.Error) {
			return &models.PrivateSubtopics{
				TotalCount:   2,
				PrivateItems: &[]models.TopicResponse{testEconomyRootTopicPrivate, testBusinessRootTopicPrivate},
			}, nil
		},
		GetTopicPrivateFunc: func(ctx context.Context, reqHeaders sdk.Headers, topicID string) (*models.TopicResponse, topicCliErr.Error) {
			switch topicID {
			case "6734":
				return &models.TopicResponse{
					ID: topicID,
					Current: &models.Topic{
						ID:          "6734",
						Title:       "Economy",
						Slug:        "economy",
						SubtopicIds: &[]string{"1834"},
					},
				}, nil
			case "1834":
				return &models.TopicResponse{
					ID: topicID,
					Current: &models.Topic{
						ID:          "1834",
						Title:       "Environmental Accounts",
						Slug:        "environmentalaccounts",
						SubtopicIds: &[]string{},
					},
				}, nil
			case "1234":
				return &models.TopicResponse{
					ID: topicID,
					Current: &models.Topic{
						ID:          "1234",
						Title:       "Business",
						Slug:        "business",
						SubtopicIds: &[]string{},
					},
				}, nil
			default:
				return nil, topicCliErr.StatusError{
					Err: errors.New("unexpected error"),
				}
			}
		},
	}

	Convey("When LoadTopicsMap is called with enableTopicTagging is enabled and root topics are retrieved and processed successfully", t, func() {
		topicMap, err := LoadTopicsMap(ctx, serviceAuthToken, mockClient)
		So(err, ShouldBeNil)
		So(len(topicMap), ShouldEqual, 3)
		So(topicMap["6734"].Slug, ShouldEqual, "economy")
		So(topicMap["1234"].Slug, ShouldEqual, "business")
	})

	Convey("When LoadTopicsMap is called with enableTopicTagging is enabled and GetRootTopicsPrivate returns an error", t, func() {
		mockClient = &mockTopic.ClienterMock{
			GetRootTopicsPrivateFunc: func(ctx context.Context, reqHeaders sdk.Headers) (*models.PrivateSubtopics, topicCliErr.Error) {
				return nil, topicCliErr.StatusError{
					Err: errors.New("unexpected error"),
				}
			},
		}
		topicMap, err := LoadTopicsMap(ctx, serviceAuthToken, mockClient)
		So(err, ShouldNotBeNil)
		So(len(topicMap), ShouldEqual, 0)
	})

	Convey("When LoadTopicsMap is called with enableTopicTagging is enabled and GetRootTopicsPrivate returns nil PrivateItems", t, func() {
		mockClient = &mockTopic.ClienterMock{
			GetRootTopicsPrivateFunc: func(ctx context.Context, reqHeaders sdk.Headers) (*models.PrivateSubtopics, topicCliErr.Error) {
				return &models.PrivateSubtopics{
					TotalCount:   1,
					PrivateItems: nil,
				}, nil
			},
		}
		topicMap, err := LoadTopicsMap(ctx, serviceAuthToken, mockClient)
		So(err, ShouldNotBeNil)
		So(len(topicMap), ShouldEqual, 0)
	})
}
