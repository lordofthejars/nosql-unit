package com.lordofthejars.nosqlunit.couchbase;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

import java.io.InputStream;

public class DefaultCouchbaseComparisonStrategy implements com.lordofthejars.nosqlunit.core
        .ComparisonStrategy<CouchBaseClientCallback> {

    @Override
    public boolean compare(final CouchBaseClientCallback connection, final InputStream dataset) throws NoSqlAssertionError,
            Throwable {
        CouchbaseAssertion.strictAssertEquals(dataset, connection.couchBaseClient());
        return true;
    }
}
