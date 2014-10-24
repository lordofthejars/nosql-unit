package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

import java.io.InputStream;

public class CouchbaseOperation extends AbstractCustomizableDatabaseOperation<CouchBaseClientCallback, CouchbaseClient> {

    private final CouchbaseClient couchbaseClient;

    public CouchbaseOperation(final CouchbaseClient client) {
        couchbaseClient = client;
        setInsertionStrategy(new DefaultCouchbaseInsertionStrategy());
        setComparisonStrategy(new DefaultCouchbaseComparisonStrategy());
    }

    @Override
    public void insert(final InputStream dataScript) {
        insertData(dataScript);
    }

    private void insertData(final InputStream dataScript) {
        try {
            executeInsertion(new CouchBaseClientCallback() {

                @Override
                public CouchbaseClient couchBaseClient() {
                    return couchbaseClient;
                }
            }, dataScript);
        } catch (final Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deleteAll() {
        removeDatabase();
    }

    private void removeDatabase() {
        couchbaseClient.flush();
    }

    @Override
    public boolean databaseIs(final InputStream expectedData) {
        return compareData(expectedData);
    }

    private boolean compareData(final InputStream expectedData) throws NoSqlAssertionError {
        try {
            return executeComparison(new CouchBaseClientCallback() {

                @Override
                public CouchbaseClient couchBaseClient() {
                    return couchbaseClient;
                }
            }, expectedData);
        } catch (final NoSqlAssertionError e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public CouchbaseClient connectionManager() {
        return couchbaseClient;
    }
}
