package task

import (
	"context"
	"errors"

	"github.com/ONSdigital/dp-topic-api/models"
	topicCli "github.com/ONSdigital/dp-topic-api/sdk"
	"github.com/ONSdigital/log.go/v2/log"
)

type Topic struct {
	ID              string
	Slug            string
	LocaliseKeyName string
	ParentID        string
	ParentSlug      string
}

// LoadTopicsMap is a function to load the topic map in publishing (private) mode.
// This function talks to the dp-topic-api via its private endpoints to retrieve the root topic and its subtopic ids
// The data returned by the dp-topic-api is of type *models.PrivateSubtopics which is then transformed in this function for the controller
// If an error has occurred, this is captured in log.Error and then an empty map is returned
func LoadTopicsMap(ctx context.Context, serviceAuthToken string, topicClient topicCli.Clienter) map[string]Topic {
	processedTopics := make(map[string]struct{})
	topicMap := make(map[string]Topic)

	// get root topic from dp-topic-api
	rootTopic, err := topicClient.GetRootTopicsPrivate(ctx, topicCli.Headers{ServiceAuthToken: "Bearer " + serviceAuthToken})
	if err != nil {
		log.Error(ctx, "failed to get root topic from topic-api", err, log.Data{
			"req_headers": topicCli.Headers{},
		})
		return topicMap
	}

	// dereference root topics items to allow ranging through them
	var rootTopicItems []models.TopicResponse
	if rootTopic.PrivateItems != nil {
		rootTopicItems = *rootTopic.PrivateItems
	} else {
		err := errors.New("root topic private items is nil")
		log.Error(ctx, "failed to dereference root topic items pointer", err)
		return topicMap
	}

	// recursively process topics and their subtopics
	for i := range rootTopicItems {
		processTopic(ctx, serviceAuthToken, topicClient, rootTopicItems[i].ID, topicMap, processedTopics, "", "", 0)
	}

	// Check if any topics were found
	if len(topicMap) == 0 {
		err := errors.New("root topic found, but no subtopics were returned")
		log.Error(ctx, "No topics loaded into map - root topic found, but no subtopics were returned", err)
	}
	return topicMap
}

func processTopic(ctx context.Context, serviceAuthToken string, topicClient topicCli.Clienter, topicID string, topicMap map[string]Topic, processedTopics map[string]struct{}, parentTopicID, parentTopicSlug string, depth int) {
	log.Info(ctx, "Processing topic at depth", log.Data{
		"topic_id": topicID,
		"depth":    depth,
	})

	// Check if the topic has already been processed
	if _, exists := processedTopics[topicID]; exists {
		err := errors.New("topic already processed")
		log.Error(ctx, "Skipping already processed topic", err, log.Data{
			"topic_id": topicID,
			"depth":    depth,
		})
		return
	}

	// Get the topic details from the topic client
	topic, err := topicClient.GetTopicPrivate(ctx, topicCli.Headers{ServiceAuthToken: "Bearer " + serviceAuthToken}, topicID)
	if err != nil {
		log.Error(ctx, "failed to get topic details from topic-api", err, log.Data{
			"topic_id": topicID,
			"depth":    depth,
		})
		return
	}

	if topic != nil {
		// Map the current topic model to Topic struct
		topicDetails := mapTopicModelToStruct(*topic.Current, parentTopicID, parentTopicSlug)

		// Add the current topic to the topicMap
		topicMap[topic.Current.Slug] = topicDetails

		// Mark this topic as processed
		processedTopics[topicID] = struct{}{}

		// Process each subtopic recursively
		if topic.Current.SubtopicIds != nil {
			for _, subTopicID := range *topic.Current.SubtopicIds {
				processTopic(ctx, serviceAuthToken, topicClient, subTopicID, topicMap, processedTopics, topicID, topic.Current.Slug, depth+1)
			}
		}
	}
}

func mapTopicModelToStruct(topic models.Topic, parentID, parentSlug string) Topic {
	return Topic{
		ID:              topic.ID,
		Slug:            topic.Slug,
		LocaliseKeyName: topic.Title,
		ParentID:        parentID,
		ParentSlug:      parentSlug,
	}
}
