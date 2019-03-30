package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_COLLECTION;
import static java.util.Arrays.asList;

public final class MarkLogicOperation extends AbstractCustomizableDatabaseOperation<MarkLogicConnectionCallback, DatabaseClient> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MarkLogicOperation.class);

    private DatabaseClient databaseClient;

    private MarkLogicConfiguration marklogicConfiguration;

    public MarkLogicOperation(MarkLogicConfiguration marklogicConfiguration) {
        this(marklogicConfiguration.getDatabaseClient(), marklogicConfiguration, null);
    }

    public MarkLogicOperation(MarkLogicConfiguration marklogicConfiguration, Object target) {
        this(marklogicConfiguration.getDatabaseClient(), marklogicConfiguration, target);
    }

    protected MarkLogicOperation(DatabaseClient databaseClient, MarkLogicConfiguration marklogicConfiguration, Object target) {
        this.databaseClient = databaseClient;
        this.marklogicConfiguration = marklogicConfiguration;
        setInsertionStrategy(new DefaultInsertionStrategy(target));
        setComparisonStrategy(new DefaultComparisonStrategy(target));
    }

    @Override
    public void insert(InputStream contentStream) {
        insertData(contentStream);
    }

    private void insertData(InputStream contentStream) {
        try {
            executeInsertion(new MarkLogicConnectionCallback() {
                @Override
                public DatabaseClient databaseClient() {
                    return databaseClient;
                }
            }, contentStream);
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error inserting data set file.", e);
        }
    }

    @Override
    public void deleteAll() {
        deleteAllElements(databaseClient);
    }

    void setTarget(Object target) {
        ((DefaultInsertionStrategy) insertionStrategy).setTarget(target);
        ((DefaultComparisonStrategy) comparisonStrategy).setTarget(target);
    }

    private void deleteAllElements(DatabaseClient databaseClient) {
        String directory = marklogicConfiguration.getCleanDirectory();
        QueryManager queryManager = databaseClient.newQueryManager();
        //delete data seeded by framework in any case
        Set<String> collections = new HashSet<>(asList(marklogicConfiguration.getCleanCollections()));
        if (!collections.isEmpty()) {
            collections.add(DEFAULT_COLLECTION);
            //see: https://stackoverflow.com/questions/53523500/failed-to-delete-multiple-collections
            collections.forEach(c -> {
                DeleteQueryDefinition deleteQuery = queryManager.newDeleteDefinition();
                deleteQuery.setDirectory(directory);
                deleteQuery.setCollections(c);
                queryManager.delete(deleteQuery);
            });
        } else {
            DeleteQueryDefinition deleteQuery = queryManager.newDeleteDefinition();
            deleteQuery.setDirectory(directory);
            queryManager.delete(deleteQuery);
        }
    }

    @Override
    public boolean databaseIs(InputStream contentStream) {
        return compareData(contentStream);
    }

    private boolean compareData(InputStream contentStream) throws NoSqlAssertionError {
        try {
            executeComparison(new MarkLogicConnectionCallback() {
                @Override
                public DatabaseClient databaseClient() {
                    return databaseClient;
                }
            }, contentStream);
            return true;
        } catch (NoSqlAssertionError e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading expected data set file.", e);
        }
    }

    @Override
    public DatabaseClient connectionManager() {
        return databaseClient;
    }
}
