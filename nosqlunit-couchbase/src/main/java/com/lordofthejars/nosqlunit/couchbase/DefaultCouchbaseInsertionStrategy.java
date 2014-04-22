package com.lordofthejars.nosqlunit.couchbase;

import java.io.InputStream;

public class DefaultCouchbaseInsertionStrategy implements com.lordofthejars.nosqlunit.core.InsertionStrategy<CouchBaseClientCallback> {

    @Override
    public void insert(final CouchBaseClientCallback connection, final InputStream dataset) throws Throwable {
        final DataLoader dataLoader = new DataLoader(connection.couchBaseClient());
        insertDocuments(dataLoader, dataset);
    }

    private void insertDocuments(final DataLoader dataLoader, final InputStream dataScript) {
        dataLoader.load(dataScript);
    }
}
