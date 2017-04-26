package com.lordofthejars.nosqlunit.couchdb;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import java.io.InputStream;
import org.ektorp.CouchDbConnector;
import org.ektorp.http.HttpClient;
import org.ektorp.http.RestTemplate;

public class CouchDbOperation extends AbstractCustomizableDatabaseOperation<CouchDbConnectionCallback, CouchDbConnector> {

    private CouchDbConnector couchDbConnector;

    public CouchDbOperation(CouchDbConnector couchDbConnector) {
        this.couchDbConnector = couchDbConnector;
        setInsertionStrategy(new DefaultCouchDbInsertionStrategy());
        setComparisonStrategy(new DefaultCouchDbComparisonStrategy());
    }

    @Override
    public void insert(InputStream dataScript) {
        insertData(dataScript);
    }

    private void insertData(InputStream dataScript) {
        try {
            executeInsertion(new CouchDbConnectionCallback() {

                @Override
                public CouchDbConnector couchDbConnector() {
                    return couchDbConnector;
                }
            }, dataScript);
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deleteAll() {
        removeDatabase();
    }

    private void removeDatabase() {
        HttpClient connection = couchDbConnector.getConnection();
        RestTemplate restTemplate = new RestTemplate(connection);
        restTemplate.delete(couchDbConnector.path());

        couchDbConnector.createDatabaseIfNotExists();
    }

    @Override
    public boolean databaseIs(InputStream expectedData) {
        return compareData(expectedData);
    }

    private boolean compareData(InputStream expectedData) throws NoSqlAssertionError {
        try {
            return executeComparison(new CouchDbConnectionCallback() {

                @Override
                public CouchDbConnector couchDbConnector() {
                    return couchDbConnector;
                }
            }, expectedData);
        } catch (NoSqlAssertionError e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public CouchDbConnector connectionManager() {
        return this.couchDbConnector;
    }
}
