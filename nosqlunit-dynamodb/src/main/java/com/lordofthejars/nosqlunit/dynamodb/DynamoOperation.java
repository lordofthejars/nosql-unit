
package com.lordofthejars.nosqlunit.dynamodb;

import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public final class DynamoOperation
        extends AbstractCustomizableDatabaseOperation<DynamoDbConnectionCallback, AmazonDynamoDB> {

    private static Logger LOGGER = LoggerFactory.getLogger(DynamoOperation.class);

    private AmazonDynamoDB dynamo;

    private DynamoDbConfiguration dynamoDbConfiguration;

    protected DynamoOperation(AmazonDynamoDB dynamo, DynamoDbConfiguration dynamoDbConfiguration) {
        this.dynamo = dynamo;
        this.dynamoDbConfiguration = dynamoDbConfiguration;
        this.setInsertionStrategy(new DefaultInsertionStrategy());
        this.setComparisonStrategy(new DefaultComparisonStrategy());
    }

    public DynamoOperation(DynamoDbConfiguration dynamoDbConfiguration) {
        this.dynamo = dynamoDbConfiguration.getDynamo();
        this.dynamoDbConfiguration = dynamoDbConfiguration;
        this.setInsertionStrategy(new DefaultInsertionStrategy());
        this.setComparisonStrategy(new DefaultComparisonStrategy());
    }

    @Override
    public void insert(InputStream contentStream) {
        insertData(contentStream);
    }

    private void insertData(InputStream contentStream) {
        try {

            executeInsertion(() -> dynamo, contentStream);

        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading data set file.", e);
        }
    }

    @Override
    public void deleteAll() {
        deleteAllElements(dynamo);
    }

    private void deleteAllElements(AmazonDynamoDB dynamoDb) {
        List<String> tableNames = DynamoDBOperation.getAllTables(dynamoDb);
        DynamoDBOperation.truncateTables(dynamoDb, tableNames);
    }

    @Override
    public boolean databaseIs(InputStream contentStream) {

        return compareData(contentStream);

    }

    private boolean compareData(InputStream contentStream) throws NoSqlAssertionError {
        try {
            executeComparison(() -> dynamo, contentStream);
            return true;
        } catch (NoSqlAssertionError e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading expected data set file.", e);
        }
    }

    @Override
    public AmazonDynamoDB connectionManager() {
        return dynamo;
    }

}
