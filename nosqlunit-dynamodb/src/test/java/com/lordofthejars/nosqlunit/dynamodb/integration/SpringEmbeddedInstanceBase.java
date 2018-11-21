
package com.lordofthejars.nosqlunit.dynamodb.integration;

import static com.lordofthejars.nosqlunit.dynamodb.DynamoDbRule.DynamoDbRuleBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.dynamodb.DynamoDbRule;

public abstract class SpringEmbeddedInstanceBase {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private AmazonDynamoDB dynamo;

    @Rule
    public DynamoDbRule dynamoDbRule = newDynamoDbRule().defaultSpringDynamoDb();

    protected void validateDynamoConnection() {
        DatabaseOperation<AmazonDynamoDB> databaseOperation = dynamoDbRule.getDatabaseOperation();
        AmazonDynamoDB connectionManager = databaseOperation.connectionManager();

        assertThat(connectionManager, is(dynamo));
    }

}
