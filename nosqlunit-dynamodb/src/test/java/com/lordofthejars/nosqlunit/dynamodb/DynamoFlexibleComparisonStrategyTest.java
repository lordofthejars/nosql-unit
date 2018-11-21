
package com.lordofthejars.nosqlunit.dynamodb;

import static com.lordofthejars.nosqlunit.dynamodb.DynamoDbRule.DynamoDbRuleBuilder.*;
import static com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDb.InMemoryDynamoRuleBuilder.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.lordofthejars.nosqlunit.annotation.CustomComparisonStrategy;
import com.lordofthejars.nosqlunit.annotation.IgnorePropertyValue;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;

@CustomComparisonStrategy(comparisonStrategy = DynamoFlexibleComparisonStrategy.class)
public class DynamoFlexibleComparisonStrategyTest {

    @ClassRule
    public static final InMemoryDynamoDb IN_MEMORY_DYNAMO_DB = newInMemoryDynamoDbRule().build();

    @Rule
    public DynamoDbRule dynamoDbRule = newDynamoDbRule().defaultEmbeddedDynamoDb();

    @SuppressWarnings("deprecation")
    @BeforeClass
    public static void setup() throws Exception {
        final AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient();
        amazonDynamoDB.setEndpoint("http://localhost:8000");
        final ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(
                1L).withWriteCapacityUnits(1L);
        List<KeySchemaElement> keys = new ArrayList<>();
        keys.add(new KeySchemaElement("id", KeyType.HASH));
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition("id", ScalarAttributeType.S));
        CreateTableRequest createTableRequest = new CreateTableRequest(attributeDefinitions, "table", keys, throughput);
        amazonDynamoDB.createTable(createTableRequest);

        keys = new ArrayList<>();
        keys.add(new KeySchemaElement("value", KeyType.HASH));
        attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition("value", ScalarAttributeType.S));
        createTableRequest = new CreateTableRequest(attributeDefinitions, "another-table", keys, throughput);
        amazonDynamoDB.createTable(createTableRequest);
    }

    @AfterClass
    public static void after() {
        final AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient();
        amazonDynamoDB.setEndpoint("http://localhost:8000");

        amazonDynamoDB.deleteTable("table");
        amazonDynamoDB.deleteTable("another-table");
    }

    @Test
    @UsingDataSet(locations = "DynamoFlexibleComparisonStrategyTest#thatShowWarnings.json")
    @ShouldMatchDataSet(location = "DynamoFlexibleComparisonStrategyTest#thatShowWarnings-expected.json")
    @IgnorePropertyValue(properties = { "id", "2", "table.3" })
    public void shouldIgnorePropertiesInFlexibleStrategy() {
    }
}
