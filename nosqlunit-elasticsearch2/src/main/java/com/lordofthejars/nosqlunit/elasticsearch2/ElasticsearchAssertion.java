package com.lordofthejars.nosqlunit.elasticsearch2;

import com.lordofthejars.nosqlunit.elasticsearch2.parser.DataReader;
import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.lordofthejars.nosqlunit.util.DeepEquals;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchAssertion {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private ElasticsearchAssertion() {
		super();
	}

	public static void strictAssertEquals(List<Map<String, Object>> expectedDocuments, Client client) {

		checkNumberOfDocuments(expectedDocuments, client);

		for (Map<String, Object> document : expectedDocuments) {
			final Object object = document.get(DataReader.DOCUMENT_ELEMENT);

			if (object instanceof List) {
				@SuppressWarnings("unchecked")
				final List<Map<String, Object>> properties = (List<Map<String, Object>>) object;

				final List<GetRequestBuilder> indexes = new ArrayList<>();
				Map<String, Object> expectedDataOfDocument = new HashMap<>();

				for (Map<String, Object> property : properties) {

					if (property.containsKey(DataReader.INDEX_ELEMENT)) {
						indexes.add(prepareGetIndex(property.get(DataReader.INDEX_ELEMENT), client));
					} else {
						if (property.containsKey(DataReader.DATA_ELEMENT)) {
							expectedDataOfDocument = dataOfDocument(property.get(DataReader.DATA_ELEMENT));
						}
					}

				}

				checkIndicesWithDocument(indexes, expectedDataOfDocument);

			} else {
				throw new IllegalArgumentException("Array of Indexes and Data are required.");
			}
		}
	}

	private static void checkIndicesWithDocument(List<GetRequestBuilder> indexes,
												 Map<String, Object> expectedDataOfDocument) {
		for (GetRequestBuilder getRequestBuilder : indexes) {

			GetResponse dataOfDocumentResponse = getRequestBuilder.execute().actionGet();

			checkExistenceOfDocument(getRequestBuilder, dataOfDocumentResponse);
			checkDocumentEquality(expectedDataOfDocument, getRequestBuilder, dataOfDocumentResponse);

		}
	}

	private static void checkDocumentEquality(Map<String, Object> expectedDataOfDocument,
											  GetRequestBuilder getRequestBuilder, GetResponse dataOfDocumentResponse) {
		final Map<String, Object> dataOfDocument = new LinkedHashMap<>(dataOfDocumentResponse.getSource());

		if (!DeepEquals.deepEquals(dataOfDocument, expectedDataOfDocument)) {
			try {
				throw FailureHandler.createFailure("Expected document for index: %s - type: %s - id: %s is %s, but %s was found.", getRequestBuilder.request().index(), getRequestBuilder.request().type(),
						getRequestBuilder.request().id(), OBJECT_MAPPER.writeValueAsString(expectedDataOfDocument), OBJECT_MAPPER.writeValueAsString(dataOfDocument));
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	private static void checkExistenceOfDocument(GetRequestBuilder getRequestBuilder, GetResponse dataOfDocumentResponse) {
		if (!dataOfDocumentResponse.isExists()) {
			throw FailureHandler.createFailure(
					"Document with index: %s - type: %s - id: %s has not returned any document.",
					getRequestBuilder.request().index(), getRequestBuilder.request().type(),
					getRequestBuilder.request().id());
		}
	}

	private static void checkNumberOfDocuments(List<Map<String, Object>> expectedDocuments, Client client) {
		int expectedNumberOfElements = expectedDocuments.size();

		long numberOfInsertedDocuments = numberOfInsertedDocuments(client);

		if (expectedNumberOfElements != numberOfInsertedDocuments) {
			throw FailureHandler.createFailure("Expected number of documents are %s but %s has been found.",
					expectedNumberOfElements, numberOfInsertedDocuments);
		}
	}

	private static GetRequestBuilder prepareGetIndex(Object object, Client client) {
		@SuppressWarnings("unchecked")
		Map<String, String> indexInformation = (Map<String, String>) object;

		GetRequestBuilder prepareGet = client.prepareGet();

		if (indexInformation.containsKey(DataReader.INDEX_NAME_ELEMENT)) {
			prepareGet.setIndex(indexInformation.get(DataReader.INDEX_NAME_ELEMENT));
		}

		if (indexInformation.containsKey(DataReader.INDEX_TYPE_ELEMENT)) {
			prepareGet.setType(indexInformation.get(DataReader.INDEX_TYPE_ELEMENT));
		}

		if (indexInformation.containsKey(DataReader.INDEX_ID_ELEMENT)) {
			prepareGet.setId(indexInformation.get(DataReader.INDEX_ID_ELEMENT));
		}

		return prepareGet;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> dataOfDocument(Object object) {
		return (Map<String, Object>) object;
	}

	private static long numberOfInsertedDocuments(Client client) {
		final CountResponse numberOfElements = client.prepareCount().execute().actionGet();
		return numberOfElements.getCount();
	}
}
