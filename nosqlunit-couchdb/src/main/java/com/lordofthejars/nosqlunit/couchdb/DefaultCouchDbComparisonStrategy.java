package com.lordofthejars.nosqlunit.couchdb;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import java.io.InputStream;

public class DefaultCouchDbComparisonStrategy implements CouchDbComparisonStrategy {

    @Override
    public boolean compare(CouchDbConnectionCallback connection, InputStream dataset)
        throws NoSqlAssertionError, Throwable {
        CouchDbAssertion.strictAssertEquals(dataset, connection.couchDbConnector());
        return true;
    }

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
    }
}
