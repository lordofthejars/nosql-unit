
package com.lordofthejars.nosqlunit.dynamodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class utilities for managing database. I contains all methods to get, create and delete
 * tables thereby to insert data.
 *
 * @author Zied ANDOLSI
 *
 */
public abstract class DynamoDBOperation {

    /**
     * {@link DynamoDBOperation}'s Logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBOperation.class);

    /**
     * Returns all existing tables in database.
     *
     * @param client client used to connect to database.
     * @return list of table' name.
     */
    public static List<String> getAllTables(AmazonDynamoDB client) {
        LOGGER.debug("starting getAllTables method...");
        ListTablesResult listTables = client.listTables();
        if (listTables != null) {
            return listTables.getTableNames();
        }
        return Collections.emptyList();
    }

    /**
     * Deletes tables whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param tableNames list of table' name
     * @return true if all tables are deleted
     */
    public static boolean deleteTables(AmazonDynamoDB client, List<String> tableNames) {
        LOGGER.debug("starting deleteTables method...");
        boolean result = true;
        for (String tableName : tableNames) {
            result = result && deleteTable(client, tableName);
        }
        return result;
    }

    /**
     * Deletes table whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param tableName list of table' name
     * @return true if table is deleted
     */
    public static boolean deleteTable(AmazonDynamoDB client, String tableName) {
        LOGGER.debug("starting deleteTable method...");
        try {
            DeleteTableRequest request = new DeleteTableRequest().withTableName(tableName);
            DeleteTableResult result = client.deleteTable(request);
            return result != null;
        } catch (AmazonServiceException e) {
            LOGGER.error("error occurred when trying to delete table [ {} ] : {}", tableName, e.getLocalizedMessage());
            return false;
        }
    }

    /**
     * Creates tables whose description is received as parameter.
     *
     * @param client client used to connect to database
     * @param tables list of {@link TableDescription}
     * @return true if all tables are created
     */
    public static boolean createTables(AmazonDynamoDB client, List<TableDescription> tables) {
        LOGGER.debug("starting createTables method...");
        boolean result = true;
        for (TableDescription tableDescription : tables) {
            result = result && createTable(client, tableDescription);
        }
        return result;

    }

    /**
     * Creates table whose description is received as parameter.
     *
     * @param client client used to connect to database
     * @param tableDescription {@link TableDescription}
     * @return true if table is created
     */
    public static boolean createTable(AmazonDynamoDB client, TableDescription tableDescription) {
        try {
            CreateTableRequest createTableRequest = new CreateTableRequest();
            createTableRequest.setTableName(tableDescription.getTableName());
            createTableRequest.setAttributeDefinitions(tableDescription.getAttributeDefinitions());
            createTableRequest.setKeySchema(tableDescription.getKeySchema());
            ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput().withReadCapacityUnits(
                    tableDescription.getProvisionedThroughput().getReadCapacityUnits()).withWriteCapacityUnits(
                            tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
            createTableRequest.setProvisionedThroughput(provisionedthroughput);
            CreateTableResult result = client.createTable(createTableRequest);
            return result != null;
        } catch (AmazonServiceException e) {
            LOGGER.error("error occurred when trying to create table [ " + tableDescription.getTableName() + " ] : "
                    + e.getLocalizedMessage());
            return false;
        }
    }

    /**
     * Inserts data received in the <code>dataSetResourceFile</code>.
     *
     * @param client client used to connect to database
     * @param dataSetResourceFile data set resource file
     * @return true if all items are inserted.
     * @throws IOException thrown when error occurred while trying to map data found in
     *         <code>is</code> to dynamodb object
     */
    public static boolean insertData(AmazonDynamoDB client, InputStream dataSetResourceFile) throws IOException {
        LOGGER.debug("starting insertData method...");
        if (dataSetResourceFile == null) {
            LOGGER.error("data set file cannot be null");
            throw new IllegalArgumentException("data set file cannot be null");
        }
        boolean result = true;

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

        Map<String, List<Map<String, AttributeValue>>> parsedData = objectMapper.readValue(dataSetResourceFile,
                ExpectedDataSet.TYPE_REFERENCE);
        List<PutItemRequest> items = parsedData.entrySet() //
                .stream() //
                .flatMap(entry -> entry.getValue() //
                        .stream() //
                        .map(item -> new PutItemRequest(entry.getKey(), item))) //
                .collect(Collectors.toList());

        if (items != null && !items.isEmpty()) {
            for (PutItemRequest itemRequest : items) {
                result = result && insertItem(client, itemRequest);
            }
        }
        return result;
    }

    /**
     * Inserts item received as parameter.
     *
     * @param client client used to connect to database
     * @param itemRequest item
     * @return true if item is inserted.
     */
    public static boolean insertItem(AmazonDynamoDB client, PutItemRequest itemRequest) {
        try {
            PutItemResult itemResult = client.putItem(itemRequest);
            return itemResult != null;
        } catch (AmazonServiceException e) {
            LOGGER.error("error occurred when trying to insert item in table [ " + itemRequest.getTableName() + " ] : "
                    + e.getLocalizedMessage());
            return false;
        }

    }

    /**
     * Get all items from table whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param tableName list of table' name
     * @return An array of item attributes. Each element in this array consists of an
     *         attribute name and the value for that attribute.
     */
    public static List<Map<String, AttributeValue>> getAllItems(AmazonDynamoDB client, String tableName) {
        LOGGER.debug("starting getAllItems method...");
        List<Map<String, AttributeValue>> list = new ArrayList<>();
        Map<String, AttributeValue> lastKeyEvaluated = null;
        do {
            ScanRequest scanRequest = new ScanRequest(tableName);
            ScanResult result = client.scan(scanRequest);
            list.addAll(result.getItems());
            lastKeyEvaluated = result.getLastEvaluatedKey();
        } while (lastKeyEvaluated != null);
        return list;
    }

    /**
     * Truncates the tables whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param tableNames list of table' name
     * @return true if all tables are truncated
     */
    public static boolean truncateTables(AmazonDynamoDB client, List<String> tableNames) {
        LOGGER.debug("starting truncateTables method...");
        boolean result = true;
        for (String tableName : tableNames) {
            result = result && truncateTable(client, tableName);
        }
        return result;
    }

    /**
     * Truncates table whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param tableName list of table' name
     * @return true if table is deleted
     */
    public static boolean truncateTable(AmazonDynamoDB client, String tableName) {
        LOGGER.debug("starting truncateTable method...");
        boolean result = true;
        List<String> keys = getKeys(client, tableName);
        try {
            for (Map<String, AttributeValue> item : getAllItems(client, tableName)) {
                Map<String, AttributeValue> key = new HashMap<>();
                for (String k : keys) {
                    key.put(k, item.get(k));
                }
                DeleteItemRequest itemRequest = new DeleteItemRequest(tableName, key);
                result = result && deleteItem(client, itemRequest);
            }
            return result;
        } catch (AmazonServiceException e) {
            LOGGER.error(
                    "error occurred when trying to truncate table [ " + tableName + " ] : " + e.getLocalizedMessage());
            return false;
        }
    }

    /**
     * Gets key names of the table whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param tableName table name
     * @return An array of key names.
     */
    public static List<String> getKeys(AmazonDynamoDB client, String tableName) {
        DescribeTableResult table = client.describeTable(tableName);
        return table.getTable().getKeySchema().stream().map(KeySchemaElement::getAttributeName).collect(
                Collectors.toList());
    }

    /**
     * Deletes item received as parameter.
     *
     * @param client client used to connect to database
     * @param itemRequest item
     * @return true if item is deleted.
     */
    public static boolean deleteItem(AmazonDynamoDB client, DeleteItemRequest itemRequest) {
        try {
            DeleteItemResult itemResult = client.deleteItem(itemRequest);
            return itemResult != null;
        } catch (AmazonServiceException e) {
            LOGGER.error("error occurred when trying to delete item [ " + itemRequest.getKey() + " ] in table [ "
                    + itemRequest.getTableName() + " ] : " + e.getLocalizedMessage());
            return false;
        }

    }

}
