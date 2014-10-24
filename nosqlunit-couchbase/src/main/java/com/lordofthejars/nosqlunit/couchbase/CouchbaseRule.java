package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.CouchbaseClient;
import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class CouchbaseRule extends AbstractNoSqlTestRule {

    private static final String EXTENSION = "json";

    private DatabaseOperation<CouchbaseClient> databaseOperation;

    public CouchbaseRule(final CouchbaseConfiguration configuration) {
        super(configuration.getConnectionIdentifier());
        databaseOperation = new CouchbaseOperation(configuration.getClient());
    }

    public CouchbaseRule(final CouchbaseConfiguration configuration, final Object target) {
        super(configuration.getConnectionIdentifier());
        setTarget(target);
        databaseOperation = new CouchbaseOperation(configuration.getClient());
    }

    @Override
    public DatabaseOperation getDatabaseOperation() {
        return databaseOperation;
    }

    @Override
    public String getWorkingExtension() {
        return EXTENSION;
    }

    public static CouchbaseRule defaultRemoteCouchbase(final String bucketName) {
        final CouchbaseConfiguration configuration = RemoteCouchbaseConfigurationBuilder.Builder
                .start()
                .bucketName(bucketName)
                .build();
        return new CouchbaseRule(configuration);
    }
}
