package task

import (
	upstreamModels "github.com/ONSdigital/dis-search-upstream-stub/models"
	extractorModels "github.com/ONSdigital/dp-search-data-extractor/models"
)

const (
	ReleaseDataType = "release"
)

// MapResourceToSearchDataImport Performs default mapping of a Resource item to a SearchDataImport struct.
func MapResourceToSearchDataImport(resourceItem upstreamModels.SearchContentUpdatedResource) extractorModels.SearchDataImport {
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
		Survey:          resourceItem.Survey,
		Language:        resourceItem.Language,
		CanonicalTopic:  resourceItem.CanonicalTopic,
	}
	if resourceItem.Edition != "" {
		searchData.Edition = resourceItem.Edition
	}
	if resourceItem.ContentType == ReleaseDataType {
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
	}

	return searchData
}
