
package com.lordofthejars.nosqlunit.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class DynamoDbConfigurationBuilder {

    public static DynamoDbConfigurationBuilder dynamoDb() {
        return new DynamoDbConfigurationBuilder();
    }

    private final DynamoDbConfiguration dynamoDbConfiguration;

    private DynamoDbConfigurationBuilder() {
        dynamoDbConfiguration = new DynamoDbConfiguration();
    }

    @SuppressWarnings("deprecation")
    public DynamoDbConfiguration build() {
        final AmazonDynamoDB dynamo = new AmazonDynamoDBClient();
        dynamo.setEndpoint(dynamoDbConfiguration.getEndpoint());
        this.dynamoDbConfiguration.setDynamo(dynamo);

        return dynamoDbConfiguration;
    }

}
