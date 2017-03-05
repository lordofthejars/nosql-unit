package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.lordofthejars.nosqlunit.couchbase.model.Document;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.lordofthejars.nosqlunit.util.DeepEquals.deepEquals;

public class CouchbaseAssertion {

    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseAssertion.class);
    private static final String DESIGN_DOC_INTERNAL = "__design_doc_internal_";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void strictAssertEquals(final InputStream dataset, final Bucket bucket) {
        final Map<String, Document> expectedDocuments = DataLoader.getDocuments(dataset);
        final List<String> allDocumentIds = getAllDocumentIds(bucket);

        checkNumberOfDocuments(expectedDocuments, allDocumentIds);
        checkEachDocument(expectedDocuments, allDocumentIds, bucket);
    }

    private static void checkEachDocument(final Map<String, Document> expectedDocuments, final List<String> allDocumentIds,
                                          final Bucket bucket) {
        for (final String id : allDocumentIds) {
            final Object real = bucket.get(id);
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

    private static void checkNumberOfDocuments(final Map<String, Document> expectedDocuments, final List<String> allDocumentIds) {
        final int expectedSize = expectedDocuments.size();
        final int currentSize = allDocumentIds.size();
        if (expectedSize != currentSize) {
            throw FailureHandler.createFailure("Expected number of elements are %s but insert are %s. DB document IDS: %s",
                    expectedSize, currentSize, allDocumentIds);
        }
    }

    private static List<String> getAllDocumentIds(final Bucket bucket) {
        final String freeDesignDocName = nextUniqueViewName(bucket);

        try {
            final DesignDocument designDocAndView = createDesignDocAndView(freeDesignDocName, bucket);
            final List<String> result = getAllDocumentIds(designDocAndView, bucket);
            return result;
        } finally {
            deleteDesignDoc(freeDesignDocName, bucket);
        }
    }

    private static List<String> getAllDocumentIds(final DesignDocument designDocument, final Bucket bucket) {
        final ViewQuery query = ViewQuery.from(designDocument.name(), designDocument.views().get(0).name());
        query.stale(Stale.FALSE);
        final ViewResult result = bucket.query(query);

        return StreamSupport.stream(result.spliterator(), true).map(r -> r.id()).collect(Collectors.toList());

    }

    private static DesignDocument createDesignDocAndView(final String freeDesignDocName, final Bucket bucket) {

        final String json = "function (doc, meta) {" + System.lineSeparator() +
                "   emit(null, null);" + System.lineSeparator() +
                "}";

        final List<View> views = Arrays.asList(DefaultView.create("allDocs", json));

        final DesignDocument designDocument = DesignDocument.create(freeDesignDocName, views);

        bucket.bucketManager().insertDesignDocument(designDocument);

        return designDocument;
    }

    private static void deleteDesignDoc(final String freeDesignDocName, final Bucket bucket) {
        bucket.bucketManager().removeDesignDocument(freeDesignDocName);
    }

    private static String nextUniqueViewName(final Bucket bucket) {
        int i = 0;
        while (true) {
            final String proposal = (DESIGN_DOC_INTERNAL + (i++));
            final DesignDocument designDocument = bucket.bucketManager().getDesignDocument(proposal);
            if (designDocument == null) {
                return proposal;
            }
            LOGGER.trace("Invalid doc, keep trying. Now trying with {} " + proposal);
        }
    }
}
