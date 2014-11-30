package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.InvalidViewException;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.lordofthejars.nosqlunit.couchbase.model.Document;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.lordofthejars.nosqlunit.util.DeepEquals.deepEquals;

public class CouchbaseAssertion {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseAssertion.class);
    private static final String DESIGN_DOC_INTERNAL = "__design_doc_internal_";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void strictAssertEquals(final InputStream dataset, final CouchbaseClient couchbaseClient) {
        final Map<String, Document> expectedDocuments = DataLoader.getDocuments(dataset);
        final List<String> allDocumentIds = getAllDocumentIds(couchbaseClient);

        checkNumberOfDocuments(expectedDocuments, allDocumentIds);
        checkEachDocument(expectedDocuments, allDocumentIds, couchbaseClient);
    }

    private static void checkEachDocument(final Map<String, Document> expectedDocuments, final List<String> allDocumentIds,
                                          final CouchbaseClient couchbaseClient) {
        for (final String id : allDocumentIds) {
            final Object real = couchbaseClient.get(id);
            final Object expected = toJson(expectedDocuments.get(id).getDocument());

            if (!deepEquals(real, expected)) {
                throw FailureHandler.createFailure(
                        "Expected element # %s # is not found but # %s # was found.",
                        toJson(expected), toJson(real));
            }
        }
    }

    private static String toJson(final Object document) {
        try {
            return OBJECT_MAPPER.writeValueAsString(document);
        } catch (JsonGenerationException e) {
            throw new IllegalArgumentException(e);
        } catch (JsonMappingException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Object fromJson(final Object document) {
        if (document instanceof String) {
            String json = (String) document;
            try {
                JsonNode node = OBJECT_MAPPER.readTree(json);
                return OBJECT_MAPPER.readValue(json, String.class);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(e);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return document;
    }

    private static void checkNumberOfDocuments(final Map<String, Document> expectedDocuments, final List<String> allDocumentIds) {
        final int expectedSize = expectedDocuments.size();
        final int currentSize = allDocumentIds.size();
        if (expectedSize != currentSize) {
            throw FailureHandler.createFailure("Expected number of elements are %s but insert are %s. DB document IDS: %s",
                    expectedSize, currentSize, allDocumentIds);
        }
    }

    private static List<String> getAllDocumentIds(final CouchbaseClient couchbaseClient) {
        final String freeDesignDocName = nextUniqueViewName(couchbaseClient);

        try {
            final View allDocsView = createDesignDocAndView(freeDesignDocName, couchbaseClient);
            final List<String> result = getAllDocumentIds(allDocsView, couchbaseClient);
            return result;
        } finally {
            deleteDesignDoc(freeDesignDocName, couchbaseClient);
        }
    }

    private static List<String> getAllDocumentIds(final View allDocsView, final CouchbaseClient couchbaseClient) {
        final Query query = new Query();
        query.setStale(Stale.FALSE);
        final ViewResponse viewResults = couchbaseClient.query(allDocsView, query);

        final List<String> result = new ArrayList<String>();
        for (final ViewRow viewResult : viewResults) {
            result.add(viewResult.getId());
        }

        return result;
    }

    private static View createDesignDocAndView(final String freeDesignDocName, final CouchbaseClient couchbaseClient) {
        final DesignDocument designDocument = new DesignDocument(freeDesignDocName);
        final String json = "function (doc, meta) {\n" +
                "   emit(null, null);\n" +
                "}";

        final String viewName = "allDocs";

        designDocument.getViews().add(new ViewDesign(viewName, json));

        final Boolean designDoc = couchbaseClient.createDesignDoc(designDocument);
        if (!designDoc) {
            throw new IllegalStateException("Cannot create internal designDoc to query for all docs. Name of DesignDoc: " +
                    freeDesignDocName);
        }

        return couchbaseClient.getView(freeDesignDocName, viewName);
    }

    private static void deleteDesignDoc(final String freeDesignDocName, final CouchbaseClient couchbaseClient) {
        couchbaseClient.deleteDesignDoc(freeDesignDocName);
    }

    private static String nextUniqueViewName(final CouchbaseClient couchbaseClient) {
        int i = 0;
        while (true) {
            final String proposal = (DESIGN_DOC_INTERNAL + (i++));
            try {
                couchbaseClient.getDesignDoc(proposal);
                LOGGER.trace("Invalid doc, keep trying. Now trying with {} " + proposal);
            } catch (final InvalidViewException ignored) {
                return proposal;
            }
        }
    }
}
