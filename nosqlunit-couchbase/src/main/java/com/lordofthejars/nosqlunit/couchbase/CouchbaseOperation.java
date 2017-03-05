package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.java.Bucket;
import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

import java.io.InputStream;

public class CouchbaseOperation extends AbstractCustomizableDatabaseOperation<CouchBaseClientCallback, Bucket> {

    private final Bucket bucket;

    public CouchbaseOperation(final Bucket client) {
        bucket = client;
        setInsertionStrategy(new DefaultCouchbaseInsertionStrategy());
        setComparisonStrategy(new DefaultCouchbaseComparisonStrategy());
    }

    @Override
    public void insert(final InputStream dataScript) {
        insertData(dataScript);
    }

    private void insertData(final InputStream dataScript) {
        try {
            executeInsertion(() -> bucket, dataScript);
        } catch (final Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deleteAll() {
        removeDatabase();
    }

    private void removeDatabase() {
        bucket.bucketManager().flush();
    }

    @Override
    public boolean databaseIs(final InputStream expectedData) {
        return compareData(expectedData);
    }

    private boolean compareData(final InputStream expectedData) throws NoSqlAssertionError {
        try {
            return executeComparison(() -> bucket, expectedData);
        } catch (final NoSqlAssertionError e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Bucket connectionManager() {
        return bucket;
    }
}
