
package com.lordofthejars.nosqlunit.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public final class DynamoDbConfiguration extends AbstractJsr330Configuration {

    private static final String DEFAULT_ENDPOINT = "http://localhost:8000";

    private String endpoint = DEFAULT_ENDPOINT;

    private AmazonDynamoDB dynamo;

    public DynamoDbConfiguration() {
        super();
    }

    public DynamoDbConfiguration(String endpoint) {
        super();
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setDynamo(AmazonDynamoDB dynamo) {
        this.dynamo = dynamo;
    }

    public AmazonDynamoDB getDynamo() {
        return dynamo;
    }

}
