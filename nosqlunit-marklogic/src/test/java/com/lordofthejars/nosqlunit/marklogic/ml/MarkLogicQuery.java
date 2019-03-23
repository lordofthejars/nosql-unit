package com.lordofthejars.nosqlunit.marklogic.ml;

import com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.ExtractedResult;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Optional.ofNullable;

/**
 * Utility class, can be used to query MarkLogic databases for content via REST API.
 */
public abstract class MarkLogicQuery {

    public static final DefaultMarkLogicQueryOptions DEFAULT_OPTIONS = new DefaultMarkLogicQueryOptions();

    private static final Logger log = LoggerFactory.getLogger(MarkLogicQuery.class);

    private MarkLogicQuery() {
    }

    public static Optional<DocumentDescriptor> findOneByUri(DatabaseClient client, String uri) {
        DocumentManager documentManager = client.newDocumentManager();
        DocumentDescriptor result = documentManager.exists(uri);
        return Optional.ofNullable(result);
    }

    public static Optional<ExtractedResult> findOneByTerm(DatabaseClient client, String value) {
        ExtractedResult result = null;
        Optional<SearchHandle> handle = MarkLogicQuery.findByTerm(client, 1, value);
        if (handle.isPresent() && handle.get().getMatchResults() != null && handle.get().getMatchResults().length > 0) {
            result = handle.get().getMatchResults()[0].getExtracted();
        }
        return Optional.ofNullable(result);
    }

    public static Optional<SearchHandle> findByTerm(DatabaseClient client, long pageLength, String value) {
        QueryManager queryManager = client.newQueryManager();
        queryManager.setPageLength(pageLength);
        RawCombinedQueryDefinition query = queryManager.newRawCombinedQueryDefinition(new StringHandle(
                        "<search xmlns='http://marklogic.com/appservices/search'>" +
                                "    <options>" +
                                "        <extract-document-data selected='all'>" +
                                "            <extract-path>/*</extract-path>" +
                                "        </extract-document-data>" +
                                "    </options>" +
                                "    <query>" +
                                "        <term-query>" +
                                "            <text>" + value + "</text>" +
                                "        </term-query>" +
                                "    </query>" +
                                "</search>"
                )
        );
        query.setDirectory(DEFAULT_OPTIONS.directory);
        return ofNullable(queryManager.search(query, new SearchHandle()));
    }

    public static long countByTerm(MarkLogicConfiguration marklogicConfiguration, String value) {
        DatabaseClient client = marklogicConfiguration.getDatabaseClient();
        QueryManager queryManager = client.newQueryManager();
        StringQueryDefinition query = queryManager.newStringDefinition();
        query.setDirectory(DEFAULT_OPTIONS.directory);
        query.setCriteria(value);
        SearchHandle result = queryManager.search(query, new SearchHandle());
        return result != null ? result.getTotalResults() : 0;
    }

    public static class DefaultMarkLogicQueryOptions {

        public final String directory = "/";

        public final long start = 1;

        public final long pageLength = 2;
    }
}
