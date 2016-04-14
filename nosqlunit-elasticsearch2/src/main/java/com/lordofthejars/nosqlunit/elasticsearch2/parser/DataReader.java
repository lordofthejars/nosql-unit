package com.lordofthejars.nosqlunit.elasticsearch2.parser;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataReader {
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static final String DOCUMENTS_ELEMENT = "documents";
	public static final String DOCUMENT_ELEMENT = "document";
	public static final String DATA_ELEMENT = "data";
	public static final String INDEX_ELEMENT = "index";
	public static final String INDEX_NAME_ELEMENT = "indexName";
	public static final String INDEX_TYPE_ELEMENT = "indexType";
	public static final String INDEX_ID_ELEMENT = "indexId";

	private Client client;

	public DataReader(Client client) {
		this.client = client;
	}

	public void read(InputStream data) {
		try {
			final List<Map<String, Object>> documents = getDocuments(data);
			insertDocuments(documents);
			refreshNode();
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private void refreshNode() {
		client.admin().indices().prepareRefresh().execute().actionGet();
	}

	private void insertDocuments(List<Map<String, Object>> documents) {
		for (Map<String, Object> document : documents) {
			final Object object = document.get(DOCUMENT_ELEMENT);

			if (object instanceof List) {
				@SuppressWarnings("unchecked")
				final List<Map<String, Object>> properties = (List<Map<String, Object>>) object;
				insertDocument(properties);
			} else {
				throw new IllegalArgumentException("Array of Indexes and Data are required.");
			}
		}
	}

	private void insertDocument(List<Map<String, Object>> properties) {
		final List<IndexRequestBuilder> indexes = new ArrayList<>();
		Map<String, Object> dataOfDocument = new HashMap<>();

		for (Map<String, Object> property : properties) {
			if (property.containsKey(INDEX_ELEMENT)) {
				indexes.add(createIndex(property.get(INDEX_ELEMENT)));
			} else {
				if (property.containsKey(DATA_ELEMENT)) {
					dataOfDocument = dataOfDocument(property.get(DATA_ELEMENT));
				}
			}
		}

		insertIndexes(indexes, dataOfDocument);
	}

	private void insertIndexes(List<IndexRequestBuilder> indexes, Map<String, Object> dataOfDocument) {
		for (IndexRequestBuilder indexRequestBuilder : indexes) {
			indexRequestBuilder.setSource(dataOfDocument).execute().actionGet();
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> dataOfDocument(Object object) {
		return (Map<String, Object>) object;
	}

	private IndexRequestBuilder createIndex(Object object) {
		@SuppressWarnings("unchecked")
		final Map<String, String> indexInformation = (Map<String, String>) object;

		final IndexRequestBuilder prepareIndex = client.prepareIndex();

		if (indexInformation.containsKey(INDEX_NAME_ELEMENT)) {
			prepareIndex.setIndex(indexInformation.get(INDEX_NAME_ELEMENT));
		}

		if (indexInformation.containsKey(INDEX_TYPE_ELEMENT)) {
			prepareIndex.setType(indexInformation.get(INDEX_TYPE_ELEMENT));
		}

		if (indexInformation.containsKey(INDEX_ID_ELEMENT)) {
			prepareIndex.setId(indexInformation.get(INDEX_ID_ELEMENT));
		}

		return prepareIndex;
	}

	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getDocuments(InputStream data) throws IOException {
		final Map<String, Object> rootNode = MAPPER.readValue(data, new TypeReference<Map<String, Object>>() {
		});
		final Object dataElements = rootNode.get(DOCUMENTS_ELEMENT);

		if (dataElements instanceof List) {
			return (List<Map<String, Object>>) dataElements;
		} else {
			throw new IllegalArgumentException("Array of documents are required.");
		}
	}
}
