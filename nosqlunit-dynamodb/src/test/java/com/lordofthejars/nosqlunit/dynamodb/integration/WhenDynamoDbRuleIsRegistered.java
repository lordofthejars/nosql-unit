
package com.lordofthejars.nosqlunit.dynamodb.integration;

import static com.lordofthejars.nosqlunit.dynamodb.DynamoDbConfigurationBuilder.*;
import static com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDb.InMemoryDynamoRuleBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.dynamodb.DynamoDbConfiguration;
import com.lordofthejars.nosqlunit.dynamodb.DynamoDbRule;
import com.lordofthejars.nosqlunit.dynamodb.InMemoryDynamoDb;

public class WhenDynamoDbRuleIsRegistered {

    @ClassRule
    public static final InMemoryDynamoDb IN_MEMORY_DYNAMO_DB = newInMemoryDynamoDbRule().build();

    @After
    public void teardown() {
        deleteAllTables();
    }

    @Test(expected = NoSqlAssertionError.class)
    public void should_fail_if_expected_data_is_non_strict_equal() throws Throwable {

        final DynamoDbConfiguration dynamoDbConfiguration = dynamoDb().build();
        final DynamoDbRule remoteDynamoDbRule = new DynamoDbRule(dynamoDbConfiguration);

        createTable("table1", "id");

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_wrong_test");

        final Statement dynamodbStatement = remoteDynamoDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
        dynamodbStatement.evaluate();

    }

    @Test
    public void should_assert_if_expected_data_is_strict_equal() throws Throwable {

        final DynamoDbConfiguration dynamoDbConfiguration = dynamoDb().build();
        final DynamoDbRule remoteDynamoDbRule = new DynamoDbRule(dynamoDbConfiguration);

        createTable("table1", "id");
        createTable("table2", "id");

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_equal_test");

        final Statement dynamodbStatement = remoteDynamoDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
        dynamodbStatement.evaluate();

    }

    @Test
    public void should_clean_dataset_with_delete_all_strategy() throws Throwable {

        final DynamoDbConfiguration dynamoDbConfiguration = dynamoDb().build();
        final DynamoDbRule remoteDynamoDbRule = new DynamoDbRule(dynamoDbConfiguration);

        createTable("table1", "id");
        createTable("table2", "id");

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_delete_test");

        final Statement dynamodbStatement = remoteDynamoDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
        dynamodbStatement.evaluate();

        final Map<String, AttributeValue> currentData = findItemByKey("table1", "id", 1);
        assertThat(currentData, nullValue());
    }

    @Test
    public void should_insert_new_dataset_with_insert_strategy() throws Throwable {

        final DynamoDbConfiguration dynamoDbConfiguration = dynamoDb().build();
        final DynamoDbRule remoteDynamoDbRule = new DynamoDbRule(dynamoDbConfiguration);

        createTable("table1", "id");
        createTable("table2", "id");

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_insert_test_1");

        final MyTestClass testObject = new MyTestClass();
        final Statement dynamodbStatement = remoteDynamoDbRule.apply(noStatement, frameworkMethod, testObject);
        dynamodbStatement.evaluate();

        final Map<String, AttributeValue> currentData = findItemByKey("table1", "id", 1);
        assertThat(currentData, is(notNullValue()));
        assertThat(currentData.get("code"), is(notNullValue()));
        assertThat(currentData.get("code"), is(new AttributeValue("JSON dataset")));

        createTable("table3", "id");
        createTable("table4", "id");

        final FrameworkMethod frameworkMethod2 = frameworkMethod(MyTestClass.class, "my_insert_test_2");

        final Statement dynamodbStatement2 = remoteDynamoDbRule.apply(noStatement, frameworkMethod2, testObject);
        dynamodbStatement2.evaluate();

        final Map<String, AttributeValue> previousData = findItemByKey("table1", "id", 1);
        assertThat(previousData, is(notNullValue()));
        assertThat(previousData.get("code"), is(notNullValue()));
        assertThat(previousData.get("code"), is(new AttributeValue("JSON dataset")));

        final Map<String, AttributeValue> data = findItemByKey("table3", "id", 6);
        assertThat(data, is(notNullValue()));
        assertThat(data.get("code"), is(notNullValue()));
        assertThat(data.get("code"), is(new AttributeValue("Another row")));

    }

    @Test
    public void should_clean_previous_data_and_insert_new_dataset_with_clean_insert_strategy() throws Throwable {

        final DynamoDbConfiguration dynamoDbConfiguration = dynamoDb().build();
        final DynamoDbRule remoteDynamoDbRule = new DynamoDbRule(dynamoDbConfiguration);
        createTable("table1", "id");
        createTable("table2", "id");

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_equal_test");

        final MyTestClass testObject = new MyTestClass();

        final Statement dynamodbStatement = remoteDynamoDbRule.apply(noStatement, frameworkMethod, testObject);
        dynamodbStatement.evaluate();

        final Map<String, AttributeValue> currentData = findItemByKey("table1", "id", 1);
        assertThat(currentData, is(notNullValue()));
        assertThat(currentData.get("code"), is(notNullValue()));
        assertThat(currentData.get("code"), is(new AttributeValue("JSON dataset")));

        createTable("table3", "id");
        createTable("table4", "id");

        final FrameworkMethod frameworkMethod2 = frameworkMethod(MyTestClass.class, "my_equal_test_2");

        final Statement dynamodbStatement2 = remoteDynamoDbRule.apply(noStatement, frameworkMethod2, testObject);
        dynamodbStatement2.evaluate();

        final Map<String, AttributeValue> previousData = findItemByKey("table1", "id", 1);
        assertThat(previousData, nullValue());

        final Map<String, AttributeValue> data = findItemByKey("table3", "id", 6);
        assertThat(data, is(notNullValue()));
        assertThat(data.get("code"), is(notNullValue()));
        assertThat(data.get("code"), is(new AttributeValue("Another row")));
    }

    @SuppressWarnings("deprecation")
    private Map<String, AttributeValue> findItemByKey(final String tableName, final String parameterName,
            final Number value) {
        final AmazonDynamoDB amazonDynamoDb = new AmazonDynamoDBClient();
        amazonDynamoDb.setEndpoint("http://localhost:8000");

        final Map<String, AttributeValue> key = new HashMap<>();
        key.put(parameterName, new AttributeValue().withN(value.toString()));
        final GetItemResult result = amazonDynamoDb.getItem(tableName, key);
        return result.getItem();
    }

    private FrameworkMethod frameworkMethod(final Class<?> testClass, final String methodName) {

        try {
            final Method method = testClass.getMethod(methodName);
            return new FrameworkMethod(method);
        } catch (final SecurityException e) {
            throw new IllegalArgumentException(e);
        } catch (final NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }

    }

    @SuppressWarnings("deprecation")
    private void createTable(final String tableName, final String key) throws InterruptedException {
        final ProvisionedThroughput throughput = new ProvisionedThroughput().withReadCapacityUnits(
                1L).withWriteCapacityUnits(1L);
        final AmazonDynamoDB amazonDynamoDb = new AmazonDynamoDBClient();
        amazonDynamoDb.setEndpoint("http://localhost:8000");
        final DynamoDB dynamoDB = new DynamoDB(amazonDynamoDb);

        final List<KeySchemaElement> keys = new ArrayList<>();
        keys.add(new KeySchemaElement(key, KeyType.HASH));
        final List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(key, ScalarAttributeType.N));

        final Table table = dynamoDB.createTable(tableName, keys, attributeDefinitions, throughput);
        table.waitForActive();
    }

    @SuppressWarnings("deprecation")
    private void deleteAllTables() {
        final AmazonDynamoDB amazonDynamoDb = new AmazonDynamoDBClient();
        amazonDynamoDb.setEndpoint("http://localhost:8000");
        amazonDynamoDb.listTables().getTableNames().forEach(amazonDynamoDb::deleteTable);
    }
}

class MyTestClass {

    @Test
    @UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    @ShouldMatchDataSet(location = "json3.test")
    public void my_wrong_test() {
    }

    @Test
    @UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    @ShouldMatchDataSet(location = "json.test")
    public void my_equal_test() {
    }

    @Test
    @UsingDataSet(locations = "json2.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    public void my_equal_test_2() {
    }

    @Test
    @UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.DELETE_ALL)
    public void my_delete_test() {
    }

    @Test
    @UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.INSERT)
    public void my_insert_test_1() {
    }

    @Test
    @UsingDataSet(locations = "json2.test", loadStrategy = LoadStrategyEnum.INSERT)
    public void my_insert_test_2() {
    }

}
