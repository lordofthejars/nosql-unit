
package com.lordofthejars.nosqlunit.dynamodb;

import static com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDb.InMemoryDynamoRuleBuilder.*;
import static com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDbConfigurationBuilder.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class WhenEmbeddedDynamoDbOperationsArRequired {

    private static final String DATA = "" + "{" + "\"table1\": " + "	["
            + "		{\"id\":{ \"N\": \"1\" }, \"code\": { \"S\": \"JSON dataset\"} }" + "	]" + "}";

    @ClassRule
    public static final InMemoryDynamoDb IN_MEMORY_DYNAMO_DB = newInMemoryDynamoDbRule().build();

    @BeforeClass
    public static void setup() throws Exception {
        final AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient();
        amazonDynamoDB.setEndpoint("http://localhost:8000");
        final ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(
                1L).withWriteCapacityUnits(1L);
        final List<KeySchemaElement> keys = new ArrayList<>();
        keys.add(new KeySchemaElement("id", KeyType.HASH));
        final List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition("id", ScalarAttributeType.N));
        final CreateTableRequest createTableRequest = new CreateTableRequest(attributeDefinitions, "table1", keys,
                throughput);
        amazonDynamoDB.createTable(createTableRequest);
    }

    @AfterClass
    public static void after() {
        final AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient();
        amazonDynamoDB.setEndpoint("http://localhost:8000");
        amazonDynamoDB.deleteTable("table1");
    }

    @After
    public void tearDown() {
        final AmazonDynamoDB defaultEmbeddedInstance = EmbeddedDynamoInstancesFactory.getInstance().getDefaultEmbeddedInstance();
        DynamoDBOperation.truncateTable(defaultEmbeddedInstance, "table1");
    }

    @Test
    public void data_should_be_inserted_into_dynamodb() {

        final DynamoOperation dynamoOperation = new DynamoOperation(inMemoryDynamoDb().build());
        dynamoOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        final AmazonDynamoDB dynamo = dynamoOperation.connectionManager();
        final List<Map<String, AttributeValue>> allItems = DynamoDBOperation.getAllItems(dynamo, "table1");

        assertThat(allItems, hasSize(1));

        final Map<String, AttributeValue> object = allItems.get(0);

        assertThat(object.get("id"), is(notNullValue()));
        assertThat(object.get("id"), is(new AttributeValue().withN("1")));

        assertThat(object.get("code"), is(notNullValue()));
        assertThat(object.get("code"), is(new AttributeValue("JSON dataset")));
    }

    @Test
    public void data_should_be_removed_from_dynamo() {

        final DynamoOperation dynamoOperation = new DynamoOperation(inMemoryDynamoDb().build());
        dynamoOperation.insert(new ByteArrayInputStream(DATA.getBytes()));
        dynamoOperation.deleteAll();

        final AmazonDynamoDB dynamo = dynamoOperation.connectionManager();
        final List<Map<String, AttributeValue>> allItems = DynamoDBOperation.getAllItems(dynamo, "table1");

        assertThat(allItems, hasSize(0));
    }

    @Test
    public void data_should_be_compared_between_expected_and_current_data() {

        final DynamoOperation dynamoOperation = new DynamoOperation(inMemoryDynamoDb().build());
        dynamoOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        final boolean result = dynamoOperation.databaseIs(new ByteArrayInputStream(DATA.getBytes()));

        assertThat(result, is(true));
    }

}
