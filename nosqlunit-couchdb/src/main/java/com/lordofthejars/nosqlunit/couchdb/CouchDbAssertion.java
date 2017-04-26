package com.lordofthejars.nosqlunit.couchdb;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.CouchDbConnector;

import static com.lordofthejars.nosqlunit.util.DeepEquals.deepEquals;

public class CouchDbAssertion {

    private static final String _REV = "_rev";
    private static final String _ID = "_id";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private CouchDbAssertion() {
        super();
    }

    public static final void strictAssertEquals(InputStream expectedData, CouchDbConnector couchDb) {

        try {

            List<Map<String, Object>> expectedDocuments = DataLoader.getDocuments(expectedData);
            List<String> allDocIds = couchDb.getAllDocIds();

            checkNumberOfDocuments(expectedDocuments, allDocIds);

            List<Map<String, Object>> remainingExpectedElementsWithoutIds = checkDocumentsWithId(expectedDocuments,
                couchDb, allDocIds);
            checkDocumentsWithoutId(couchDb, allDocIds, remainingExpectedElementsWithoutIds);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static void checkNumberOfDocuments(List<Map<String, Object>> expectedDocuments, List<String> allDocIds) {
        int expectedSize = expectedDocuments.size();
        int currentSize = allDocIds.size();
        if (expectedSize != currentSize) {
            throw FailureHandler.createFailure("Expected number of elements are %s but insert are %s.",
                expectedSize, currentSize);
        }
    }

    private static void checkDocumentsWithoutId(CouchDbConnector couchDb, List<String> allDocIds,
        List<Map<String, Object>> remainingExpectedElementsWithoutIds) {

        List<Map<String, Object>> storedElementsWihtoutExpectedId = loadDocuments(allDocIds, couchDb);

        for (Map<String, Object> expectedElement : remainingExpectedElementsWithoutIds) {

            if (!isExpectedElementPresent(expectedElement, storedElementsWihtoutExpectedId)) {
                throw FailureHandler.createFailure("Expected element # %s # is not found.",
                    jsonStringify(expectedElement));
            }
        }
    }

    private static boolean isExpectedElementPresent(Map<String, Object> expectedElement,
        List<Map<String, Object>> storedElementsWihtoutExpectedId) {

        for (Map<String, Object> element : storedElementsWihtoutExpectedId) {
            if (deepEquals(expectedElement, element)) {
                return true;
            }
        }

        return false;
    }

    private static List<Map<String, Object>> loadDocuments(List<String> allDocIds, CouchDbConnector couchDb) {
        List<Map<String, Object>> loadedDocuments = new ArrayList<Map<String, Object>>();

        for (String id : allDocIds) {
            Map<String, Object> element = couchDb.get(Map.class, id);

            removeIdTag(element);
            removeRevisionTag(element);

            loadedDocuments.add(element);
        }

        return loadedDocuments;
    }

    private static List<Map<String, Object>> checkDocumentsWithId(List<Map<String, Object>> expectedDocuments,
        CouchDbConnector couchDb, List<String> allDocIds) throws IOException, JsonProcessingException {

        Iterator<Map<String, Object>> expectedDocumentsIterator = expectedDocuments.iterator();
        List<Map<String, Object>> remainingElements = new ArrayList<Map<String, Object>>();

        while (expectedDocumentsIterator.hasNext()) {

            Map<String, Object> expectedDocument = expectedDocumentsIterator.next();

            Object idNode = expectedDocument.get(_ID);

            if (isIdDefined(idNode)) {
                String expectedId = toStringValue(idNode);

                if (allDocIds.remove(expectedId)) {
                    checkDocument(couchDb, expectedDocument, expectedId);
                } else {
                    throw FailureHandler.createFailure("Document with id %s is not found.", expectedId);
                }

                expectedDocumentsIterator.remove();
            } else {
                remainingElements.add(expectedDocument);
            }
        }
        return remainingElements;
    }

    private static void checkDocument(CouchDbConnector couchDb, Map<String, Object> expectedDocument, String expectedId)
        throws Error {
        // Element must be in database
        Map<String, Object> element = couchDb.get(Map.class, expectedId);
        // We remove rev because it is impossible to know by the end
        // user
        removeRevisionTag(element);

        if (!deepEquals(element, expectedDocument)) {
            throw FailureHandler.createFailure(
                "Expected element # %s # is not found but # %s # was found.",
                jsonStringify(expectedDocument), jsonStringify(element));
        }
    }

    private static boolean isIdDefined(Object idNode) {
        return idNode != null;
    }

    private static void removeRevisionTag(Map<String, Object> element) {
        element.remove(_REV);
    }

    private static void removeIdTag(Map<String, Object> element) {
        element.remove(_ID);
    }

    private static String toStringValue(Object value) {
        if (value instanceof String) {
            return (String) value;
        } else {
            throw new IllegalArgumentException("An String value was expected.");
        }
    }

    private static String jsonStringify(Map<String, Object> document) {
        try {
            return OBJECT_MAPPER.writeValueAsString(document);
        } catch (JsonGenerationException e) {
            throw new IllegalStateException(e);
        } catch (JsonMappingException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
