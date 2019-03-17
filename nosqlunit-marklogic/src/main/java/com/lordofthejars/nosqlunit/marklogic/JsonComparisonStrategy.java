package com.lordofthejars.nosqlunit.marklogic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.marklogic.content.Content;
import com.lordofthejars.nosqlunit.marklogic.content.DataSetReader;
import com.lordofthejars.nosqlunit.marklogic.content.JsonContent;
import com.lordofthejars.nosqlunit.marklogic.content.JsonParser;
import com.marklogic.client.io.marker.ContentHandleFactory;
import org.slf4j.Logger;

import java.io.InputStream;
import java.util.*;

import static com.lordofthejars.nosqlunit.core.FailureHandler.createFailure;
import static com.marklogic.client.io.JacksonHandle.newFactory;
import static org.slf4j.LoggerFactory.getLogger;

class JsonComparisonStrategy implements MarkLogicComparisonStrategy {

    private static final Logger LOGGER = getLogger(JsonComparisonStrategy.class);

    private ObjectMapper mapper;

    private ContentHandleFactory contentHandleFactory = newFactory();

    private Set<String> ignoreProperties;

    JsonComparisonStrategy(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public boolean compare(MarkLogicConnectionCallback connection, InputStream dataSet) throws NoSqlAssertionError {
        JsonParser parser = new JsonParser(mapper);
        DataSetReader reader = new DataSetReader(connection.databaseClient().newJSONDocumentManager(), contentHandleFactory);
        Set<Content> expectedData;
        final Map<String, JsonContent> actualData = new HashMap<>();
        try {
            expectedData = parser.parse(dataSet);
            reader.read(expectedData, JsonNode.class).forEach((uri, contentHandle) -> actualData.put(uri, new JsonContent(contentHandle.get())));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new NoSqlAssertionError(e.getMessage());
        }
        if (expectedData.size() != actualData.size()) {
            throw createFailure("Expected number of documents is: %s but actual number was: %s", expectedData.size(), actualData.size());
        }
        compare(expectedData, actualData, parser);
        return true;
    }

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
        this.ignoreProperties = new HashSet(Arrays.asList(ignoreProperties));
    }

    private boolean compare(Set<Content> expectedSet, Map<String, JsonContent> actualSet, JsonParser parser) {
        boolean result = true;
        for (Content e : expectedSet) {
            JsonContent expected = (JsonContent) e;
            JsonContent actual = actualSet.get(expected.getUri());
            if (actual == null) {
                result = false;
                LOGGER.warn("Expected not available in the actual data set:\n{}", expected);
                continue;
            }
            JsonNode expectedNode = parser.node((JsonContent) expected);
            JsonNode actualNode = parser.node(actual);
            if (!expectedNode.equals(actualNode)) {
                result = false;
                LOGGER.trace("Expected and actual are not equal:\n{}\n!=\n{}", expected, actual);
            }
        }
        return result;
    }
}
