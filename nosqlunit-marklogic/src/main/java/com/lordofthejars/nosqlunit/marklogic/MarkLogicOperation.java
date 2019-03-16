package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_COLLECTION;

public final class MarkLogicOperation extends AbstractCustomizableDatabaseOperation<MarkLogicConnectionCallback, DatabaseClient> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MarkLogicOperation.class);

    private DatabaseClient databaseClient;

    private MarkLogicConfiguration marklogicConfiguration;

    public MarkLogicOperation(MarkLogicConfiguration marklogicConfiguration) {
        this(marklogicConfiguration.getDatabaseClient(), marklogicConfiguration);
    }

    protected MarkLogicOperation(DatabaseClient databaseClient, MarkLogicConfiguration marklogicConfiguration) {
        this.databaseClient = databaseClient;
        this.marklogicConfiguration = marklogicConfiguration;
        setInsertionStrategy(new DefaultInsertionStrategy());
        setComparisonStrategy(new DefaultComparisonStrategy());
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
            throw new IllegalArgumentException("Unexpected error reading data set file.", e);
        }
    }

    @Override
    public void deleteAll() {
        deleteAllElements(databaseClient);
    }

    private void deleteAllElements(DatabaseClient databaseClient) {
        QueryManager queryManager = databaseClient.newQueryManager();
        DeleteQueryDefinition deleteQuery = queryManager.newDeleteDefinition();
        deleteQuery.setCollections(DEFAULT_COLLECTION);
        queryManager.delete(deleteQuery);
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
