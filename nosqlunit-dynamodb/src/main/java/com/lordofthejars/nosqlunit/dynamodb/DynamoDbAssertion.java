
package com.lordofthejars.nosqlunit.dynamodb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.lordofthejars.nosqlunit.core.FailureHandler;

public class DynamoDbAssertion {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDbAssertion.class);

    private DynamoDbAssertion() {
        super();
    }

    public static final void strictAssertEquals(final ExpectedDataSet expectedData, final AmazonDynamoDB dynamoDb) {
        final Set<String> tableaNames = expectedData.getTables();
        final List<String> tableNames = DynamoDBOperation.getAllTables(dynamoDb);

        checkTablesName(tableaNames, tableNames);

        for (final String tableName : tableaNames) {
            checkTableObjects(expectedData, dynamoDb, tableName);
        }
    }

    private static void checkTablesName(final Set<String> expectedTableNames, final List<String> dynamodbTableNames) {
        final Set<String> allTables = new HashSet<>(dynamodbTableNames);
        allTables.addAll(expectedTableNames);

        if (allTables.size() != expectedTableNames.size() || allTables.size() != dynamodbTableNames.size()) {
            throw FailureHandler.createFailure("Expected table names are %s but insert table names are %s",
                    expectedTableNames, dynamodbTableNames);
        }

    }

    private static void checkTableObjects(final ExpectedDataSet expectedData, final AmazonDynamoDB dynamoDb,
            final String tableName) {
        final List<Map<String, AttributeValue>> dataObjects = expectedData.getDataFor(tableName);
        final List<Map<String, AttributeValue>> dbTable = DynamoDBOperation.getAllItems(dynamoDb, tableName);

        final int expectedDataObjectsCount = dataObjects.size();
        final long insertedDataObjectsCount = dbTable.size();

        if (expectedDataObjectsCount != insertedDataObjectsCount) {
            throw FailureHandler.createFailure("Expected table has %s elements but insert table has %s",
                    expectedDataObjectsCount, insertedDataObjectsCount);
        }

        for (final Map<String, AttributeValue> expectedDataObject : dataObjects) {
            dbTable.stream() //
                    .filter(map -> map.equals(expectedDataObject)) //
                    .findFirst() //
                    .orElseThrow(() -> FailureHandler.createFailure("Object # %s # is not found into table [%s]",
                            expectedDataObject.toString(), tableName));
        }

    }

    // <editor-fold desc="Flexible comparator">

    /**
     * Checks that all the expected data is present in DynamoDB.
     *
     * @param expectedData Expected data.
     * @param dynamoDb Dynamo Database.
     */
    public static void flexibleAssertEquals(final ExpectedDataSet expectedData, final String[] ignorePropertyValues,
            final AmazonDynamoDB dynamoDb) {
        // Get the expected tables
        final Set<String> tableNames = expectedData.getTables();

        // Get the current tables in dynamoDB
        final List<String> listTableNames = DynamoDBOperation.getAllTables(dynamoDb);

        // Get the concrete property names that should be ignored
        // Map<String:Table, Set<String:Property>>
        final Map<String, Set<String>> propertiesToIgnore = parseIgnorePropertyValues(tableNames, ignorePropertyValues);

        // Check expected data
        flexibleCheckTablesName(tableNames, listTableNames);
        for (final String tableName : tableNames) {
            flexibleCheckTableObjects(expectedData, dynamoDb, tableName, propertiesToIgnore);
        }
    }

    /**
     * Resolve the properties that will be ignored for each expected table.
     * <p/>
     *
     * @param ignorePropertyValues Input values defined with @IgnorePropertyValue.
     * @return Map with the properties that will be ignored for each document.
     */
    private static Map<String, Set<String>> parseIgnorePropertyValues(final Set<String> tableNames,
            final String[] ignorePropertyValues) {
        final Map<String, Set<String>> propertiesToIgnore = new HashMap<>();
        final Pattern tableAndPropertyPattern = Pattern.compile(
                "^(?!system\\.)([a-z,A-Z,_][^$\0]*)([.])([^$][^.\0]*)$");
        final Pattern propertyPattern = Pattern.compile("^([^$][^.0]*)$");

        for (final String ignorePropertyValue : ignorePropertyValues) {
            final Matcher tableAndPropertyMatcher = tableAndPropertyPattern.matcher(ignorePropertyValue);
            final Matcher propertyMatcher = propertyPattern.matcher(ignorePropertyValue);

            // If the property to ignore includes the table, add it to only exclude
            // the property in the indicated table
            if (tableAndPropertyMatcher.matches()) {
                // Add the property to ignore to the proper table
                final String tableName = tableAndPropertyMatcher.group(1);
                final String propertyName = tableAndPropertyMatcher.group(3);

                if (tableNames.contains(tableName)) {
                    Set<String> properties = propertiesToIgnore.get(tableName);
                    if (properties == null) {
                        properties = new HashSet<>();
                    }
                    properties.add(propertyName);
                    propertiesToIgnore.put(tableName, properties);
                } else {
                    LOGGER.warn("Table {} for {} is not defined as expected. It won't be used for ignoring properties",
                            tableName, ignorePropertyValue);
                }
                // If the property to ignore doesn't include the table, add it to
                // all the expected tables
            } else if (propertyMatcher.matches()) {
                final String propertyName = propertyMatcher.group(0);

                // Add the property to ignore to all the expected tables
                for (final String tableName : tableNames) {
                    Set<String> properties = propertiesToIgnore.get(tableName);
                    if (properties == null) {
                        properties = new HashSet<>();
                    }
                    properties.add(propertyName);
                    propertiesToIgnore.put(tableName, properties);
                }
                // If doesn't match any pattern
            } else {
                LOGGER.warn("Property {} has an invalid table.property value. It won't be used for ignoring properties",
                        ignorePropertyValue);
            }
        }

        return propertiesToIgnore;
    }

    /**
     * Checks that all the expected table names are present in DynamoDB. Does not check
     * that all the table names present in Dynamo are in the expected dataset table names.
     * <p/>
     * If any expected table isn't found in the database table, the returned error
     * indicates only the missing expected tables.
     *
     * @param expectedTableNames Expected table names.
     * @param dynamodbTableNames Current DynamoDB table names.
     */
    private static void flexibleCheckTablesName(final Set<String> expectedTableNames,
            final List<String> dynamodbTableNames) {
        boolean ok = true;
        final HashSet<String> notFoundTableNames = new HashSet<>();
        for (final String expectedTableName : expectedTableNames) {
            if (!dynamodbTableNames.contains(expectedTableName)) {
                ok = false;
                notFoundTableNames.add(expectedTableName);
            }
        }

        if (!ok) {
            throw FailureHandler.createFailure(
                    "The following table names %s were not found in the inserted table names", notFoundTableNames);
        }
    }

    /**
     * Checks that each expected object in the table exists in the database.
     *
     * @param expectedData Expected data.
     * @param dynamoDb dynamo database.
     * @param tableName Table name.
     */
    private static void flexibleCheckTableObjects(final ExpectedDataSet expectedData, final AmazonDynamoDB dynamoDb,
            final String tableName, final Map<String, Set<String>> propertiesToIgnore) {
        final List<Map<String, AttributeValue>> dataObjects = expectedData.getDataFor(tableName);

        final List<Map<String, AttributeValue>> dbTable = DynamoDBOperation.getAllItems(dynamoDb, tableName);

        for (final Map<String, AttributeValue> expectedDataObject : dataObjects) {
            final Map<String, AttributeValue> filteredExpectedDataObject = filterProperties(expectedDataObject,
                    propertiesToIgnore.get(tableName));
            final List<Map<String, AttributeValue>> foundObjects = dbTable.stream() //
                    .map(foundDataObject -> filterProperties(foundDataObject, propertiesToIgnore.get(tableName))) //
                    .filter(map -> map.equals(filteredExpectedDataObject)) //
                    .collect(Collectors.toList());

            if (foundObjects.size() > 1) {
                LOGGER.warn(
                        "There were found {} possible matches for this object # {} #. That could have been caused by ignoring too many properties.",
                        foundObjects.size(), expectedDataObject);
            }

            if (foundObjects.isEmpty()) {
                throw FailureHandler.createFailure("Object # %s # is not found into table [%s]",
                        filteredExpectedDataObject.toString(), tableName);
            }

        }
    }

    /**
     * Removes the properties defined with @IgnorePropertyValue annotation.
     *
     * @param dataObject Object to filter.
     * @param propertiesToIgnore Properties to filter
     * @return Data object without the properties to be ignored.
     */
    private static Map<String, AttributeValue> filterProperties(final Map<String, AttributeValue> dataObject,
            final Set<String> propertiesToIgnore) {
        final Map<String, AttributeValue> filteredDataObject = new HashMap<>();

        for (final Map.Entry<String, AttributeValue> entry : dataObject.entrySet()) {
            if (propertiesToIgnore == null || !propertiesToIgnore.contains(entry.getKey())) {
                filteredDataObject.put(entry.getKey(), entry.getValue());
            }
        }

        return filteredDataObject;
    }

    // </editor-fold desc="Flexible comparator">
}
