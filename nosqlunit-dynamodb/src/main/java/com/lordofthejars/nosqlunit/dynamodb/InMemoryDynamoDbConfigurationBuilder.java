
package com.lordofthejars.nosqlunit.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.lordofthejars.nosqlunit.core.FailureHandler;

public class InMemoryDynamoDbConfigurationBuilder {

    private DynamoDbConfiguration dynamoDbConfiguration;

    public static InMemoryDynamoDbConfigurationBuilder inMemoryDynamoDb() {
        return new InMemoryDynamoDbConfigurationBuilder();
    }

    private InMemoryDynamoDbConfigurationBuilder() {
        this.dynamoDbConfiguration = new DynamoDbConfiguration();
    }

    public InMemoryDynamoDbConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
        this.dynamoDbConfiguration.setConnectionIdentifier(connectionIdentifier);
        return this;
    }

    public DynamoDbConfiguration build() {

        AmazonDynamoDB embeddedDynamo = EmbeddedDynamoInstancesFactory.getInstance().getDefaultEmbeddedInstance();

        if (embeddedDynamo == null) {
            throw FailureHandler.createIllegalStateFailure(
                    "There is no EmbeddedDynamo rule with default target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
        }

        this.dynamoDbConfiguration.setDynamo(embeddedDynamo);
        return this.dynamoDbConfiguration;

    }

}
