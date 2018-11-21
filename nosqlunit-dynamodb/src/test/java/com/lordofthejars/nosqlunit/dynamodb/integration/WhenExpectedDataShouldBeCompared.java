
package com.lordofthejars.nosqlunit.dynamodb.integration;

import static com.lordofthejars.nosqlunit.dynamodb.DynamoDbConfigurationBuilder.*;
import static com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDb.InMemoryDynamoRuleBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.dynamodb.DynamoDBOperation;
import com.lordofthejars.nosqlunit.dynamodb.DynamoDbConfiguration;
import com.lordofthejars.nosqlunit.dynamodb.DynamoOperation;
import com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDb;

public class WhenExpectedDataShouldBeCompared {

    @ClassRule
    public static final InMemoryDynamoDb IN_MEMORY_DYNAMO_DB = newInMemoryDynamoDbRule().build();

    private static DynamoOperation dynamoOperation;

    @BeforeClass
    public static final void startUp() {
        DynamoDbConfiguration dynamoConfiguration = dynamoDb().build();
        dynamoOperation = new DynamoOperation(dynamoConfiguration);
    }

    @Before
    public void setUp() {
        dropAllTables(getDynamoDB());
    }

    @Test
    public void empty_database_and_empty_expectation_should_be_equals() {

        boolean isEquals = dynamoOperation.databaseIs(new ByteArrayInputStream("{}".getBytes()));

        assertThat(isEquals, is(true));
    }

    @Test
    public void empty_expected_table_and_database_table_with_content_should_fail() throws Exception {
        try {

            addTableWithData(getDynamoDB(), "col1", "name", "Alex");

            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table has 0 elements but insert table has 1"));
        }
    }

    @Test
    public void empty_expected_table_and_empty_database_table_should_be_equal() throws Exception {

        createTable(getDynamoDB(), "col1", "id");

        boolean isEquals = dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[]}".getBytes("UTF-8")));
        assertThat(isEquals, is(true));
    }

    @Test
    public void empty_expected_table_and_empty_database_should_fail() throws Exception {
        try {

            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table names are [col1] but insert table names are []"));
        }

    }

    @Test
    public void empty_expectation_and_empty_database_table_should_fail() throws Exception {
        try {
            createTable(getDynamoDB(), "col1", "id");
            dynamoOperation.databaseIs(new ByteArrayInputStream("{}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table names are [] but insert table names are [col1]"));
        }

    }

    @Test
    public void empty_expected_table_and_empty_database_table_with_different_names_should_fail() throws Exception {
        try {
            createTable(getDynamoDB(), "col1", "id");

            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col2\":[]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table names are [col2] but insert table names are [col1]"));
        }

    }

    @Test
    public void expected_table_and_database_table_with_same_content_should_be_equals() throws Exception {

        addTableWithData(getDynamoDB(), "col1", "name", "Alex");

        boolean isEquals = dynamoOperation.databaseIs(
                new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
        assertThat(isEquals, is(true));

    }

    @Test
    public void expected_table_and_database_table_with_different_content_should_fail() throws Exception {
        try {
            addTableWithData(getDynamoDB(), "col1", "name", "Alex");

            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Soto\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Object # {name={S: Soto,}} # is not found into table [col1]"));
        }

    }

    @Test
    public void expected_table_with_content_and_database_table_empty_should_fail() throws Exception {

        try {
            createTable(getDynamoDB(), "col1", "id");

            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table has 1 elements but insert table has 0"));
        }

    }

    @Test
    public void expected_table_and_database_table_with_different_names_should_fail() throws Exception {
        try {
            addTableWithData(getDynamoDB(), "col1", "name", "Alex");

            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col2\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table names are [col2] but insert table names are [col1]"));
        }

    }

    @Test
    public void less_expected_table_than_database_table_should_fail() throws Exception {
        try {
            addTableWithData(getDynamoDB(), "col1", "name", "Alex");
            addTableWithData(getDynamoDB(), "col3", "name", "Alex");

            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table names are [col1] but insert table names are [col1, col3]"));
        }

    }

    @Test
    public void expected_table_has_some_items_different_than_database_table_items_should_fail() throws Exception {
        try {
            addTableWithData(getDynamoDB(), "col1", "name", "Alex");

            dynamoOperation.databaseIs(new ByteArrayInputStream(
                    "{\"col1\":[{\"name\":\"Alex\"}, {\"name\":\"Soto\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table has 2 elements but insert table has 1"));
        }

    }

    @Test
    public void expected_table_item_has_more_attributes_than_database_table_item_attributes_should_fail()
            throws Exception {
        try {
            // Inserted one element
            addTableWithData(getDynamoDB(), "col1", "name", "Alex");

            // Expected with two elements
            dynamoOperation.databaseIs(new ByteArrayInputStream(
                    "{\"col1\":[{\"name\":\"Alex\", \"surname\":\"Soto\"}]}".getBytes("UTF-8")));
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Object # {name={S: Alex,}, surname={S: Soto,}} # is not found into table [col1]"));
        }

    }

    @Test
    public void expected_table_item_has_same_attributes_as_database_table_item_attributes_but_different_values_should_fail()
            throws Exception {
        try {
            // Inserted one element
            addTableWithTwoData(getDynamoDB(), "col1", "name", "Alex", "surname", "Sot");

            // Expected with two elements
            dynamoOperation.databaseIs(new ByteArrayInputStream(
                    "{\"col1\":[{\"name\":\"Alex\", \"surname\":\"Soto\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Object # {name={S: Alex,}, surname={S: Soto,}} # is not found into table [col1]"));
        }

    }

    @Test
    public void expected_table_item_has_less_attributes_than_database_table_item_attributes_should_fail()
            throws Exception {
        try {
            // Inserted one element
            addTableWithTwoData(getDynamoDB(), "col1", "name", "Alex", "surname", "Soto");

            // Expected with two elements
            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Object # {name={S: Alex,}} # is not found into table [col1]"));
        }

    }

    @Test
    public void expected_table_has_all_items_different_than_database_table_items_should_fail() throws Exception {
        try {
            addTableWithData(getDynamoDB(), "col1", "name", "Alex");

            dynamoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Soto\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Object # {name={S: Soto,}} # is not found into table [col1]"));
        }

    }

    @Test
    public void more_expected_table_than_database_table_should_fail() throws Exception {
        try {
            addTableWithData(getDynamoDB(), "col1", "name", "Alex");

            dynamoOperation.databaseIs(new ByteArrayInputStream(
                    "{\"col1\":[{\"name\":\"Alex\"}], \"col3\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
            fail();
        } catch (NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected table names are [col1, col3] but insert table names are [col1]"));
        }

    }

    private void createTable(AmazonDynamoDB amazonDynamoDB, String tableName, String key) throws InterruptedException {
        final ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(
                1L).withWriteCapacityUnits(1L);
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);

        List<KeySchemaElement> keys = new ArrayList<>();
        keys.add(new KeySchemaElement(key, KeyType.HASH));
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(key, ScalarAttributeType.S));
        Table table = dynamoDB.createTable(tableName, keys, attributeDefinitions, throughput);
        table.waitForActive();
    }

    private void addTableWithTwoData(AmazonDynamoDB dynamoDb, String tableName, String field, String value,
            String field2, String value2) throws InterruptedException {
        createTable(dynamoDb, tableName, field);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(field, new AttributeValue(value));
        item.put(field2, new AttributeValue(value2));
        dynamoDb.putItem(new PutItemRequest(tableName, item));
    }

    private void addTableWithData(AmazonDynamoDB dynamoDb, String tableName, String field, String value)
            throws InterruptedException {
        createTable(dynamoDb, tableName, field);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(field, new AttributeValue(value));
        dynamoDb.putItem(new PutItemRequest(tableName, item));
    }

    private void dropAllTables(AmazonDynamoDB dynamoDb) {
        List<String> allTableNames = DynamoDBOperation.getAllTables(dynamoDb);
        DynamoDBOperation.deleteTables(dynamoDb, allTableNames);
    }

    private AmazonDynamoDB getDynamoDB() {
        return dynamoOperation.connectionManager();
    }

}
