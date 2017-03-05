package com.lordofthejars.nosqlunit.couchbase;

import com.couchbase.client.java.Bucket;
import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class CouchbaseRule extends AbstractNoSqlTestRule {

    private static final String EXTENSION = "json";

    private DatabaseOperation<Bucket> databaseOperation;

    public CouchbaseRule(final CouchbaseConfiguration configuration) {
        super(configuration.getConnectionIdentifier());
        databaseOperation = new CouchbaseOperation(configuration.getBucket());
    }

    public CouchbaseRule(final CouchbaseConfiguration configuration, final Object target) {
        super(configuration.getConnectionIdentifier());
        setTarget(target);
        databaseOperation = new CouchbaseOperation(configuration.getBucket());
    }

    @Override
    public DatabaseOperation getDatabaseOperation() {
        return databaseOperation;
    }

    @Override
    public String getWorkingExtension() {
        return EXTENSION;
    }

    @Override
    public void close() {
        databaseOperation.connectionManager().close();
    }

    public static CouchbaseRule defaultRemoteCouchbase(final String bucketName) {
        final CouchbaseConfiguration configuration = RemoteCouchbaseConfigurationBuilder.Builder
                .start()
                .bucketName(bucketName)
                .build();
        return new CouchbaseRule(configuration);
    }
}
