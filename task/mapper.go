package task

import (
	"context"

	"github.com/ONSdigital/log.go/v2/log"

	upstreamModels "github.com/ONSdigital/dis-search-upstream-stub/models"
	extractorModels "github.com/ONSdigital/dp-search-data-extractor/models"
)

const (
	ReleaseDataType = "release"
)

// MapResourceToSearchDataImport Performs default mapping of a Resource item to a SearchDataImport struct.
func MapResourceToSearchDataImport(resourceItem upstreamModels.Resource) extractorModels.SearchDataImport {
	searchData := extractorModels.SearchDataImport{
		UID:             resourceItem.URI,
		URI:             resourceItem.URI,
		DataType:        resourceItem.ContentType,
		CDID:            resourceItem.CDID,
		DatasetID:       resourceItem.DatasetID,
		MetaDescription: resourceItem.MetaDescription,
		Summary:         resourceItem.Summary,
		ReleaseDate:     resourceItem.ReleaseDate,
		Title:           resourceItem.Title,
		Topics:          resourceItem.Topics,
	}
	if resourceItem.Edition != "" {
		searchData.Edition = resourceItem.Edition
	}
	if resourceItem.ContentType == ReleaseDataType {
		logData := log.Data{
			"resourceRCData": resourceItem,
		}
		log.Info(context.Background(), "resource release calender data", logData)
		for _, data := range resourceItem.DateChanges {
			searchData.DateChanges = append(searchData.DateChanges, extractorModels.ReleaseDateDetails{
				ChangeNotice: data.ChangeNotice,
				Date:         data.PreviousDate,
			})
		}
		searchData.Published = resourceItem.Published
		searchData.Cancelled = resourceItem.Cancelled
		searchData.Finalised = resourceItem.Finalised
		searchData.ProvisionalDate = resourceItem.ProvisionalDate
		searchData.Survey = resourceItem.Survey
		searchData.Language = resourceItem.Language
		searchData.CanonicalTopic = resourceItem.CanonicalTopic
	}

	return searchData
}
