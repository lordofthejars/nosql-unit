
package com.lordofthejars.nosqlunit.dynamodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Comparison strategy that checks that all the expected data exists in the Dynamo
 * database. It doesn't compare that all the data stored in the database is included in
 * the expected file, so other data not defined in the expected resource could exist in
 * Dynamo. It just assure that the expected data exists.
 * <p>
 * Checks the following assertions:
 * <li>
 * <ul>
 * Checks that all the expected tables are present in Dynamo DB, but accepts other tables
 * stored in the database that are not defined in the expected file.
 * </ul>
 * <ul>
 * Checks that all the expected objects are present in Dynamo DB, but accepts other
 * objects stored in the same tables that are not defined as expected.
 * </ul>
 * <ul>
 * For each object checks that all properties set to be ignored its value exist in the
 * object stored in the database, but it accepts any saved value.
 * </ul>
 * </li>
 *
 * The annotation '@IgnorePropertyValue(properties = {String...})' allows the user define
 * the properties that should be ignored when checking the expected objects.
 *
 * It accepts two formats for property definition:
 * <li>
 * <ol>
 * table.property : When are defined both table and property name the exclusion will only
 * affect to the indicated table. e.g: With @IgnorePropertyValue(properties =
 * {"book.date"}), the property date will be ignored in each object of the 'book' table.
 * If other objects in different tables have the property 'date' it won't be ignored.
 * </ol>
 * <ol>
 * property : When only is defined the property name it will be excluded from all objects
 * in any expected table. e.g: With @IgnorePropertyValue(properties = {"date"}), the
 * property 'date' will be ignored in each object, no matter the table.
 * </ol>
 * </li>
 * </p>
 *
 * The values of the properties to be ignored should be named following the rules for
 * valid table and property names defined in <a href=
 * "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html">
 * Naming Rules and Data Types</a> document.
 *
 * When more than one object stored in the database matches the expected object after
 * ignoring properties a warning is shown notifying the number of objects that were found.
 *
 * @author <a mailto="faisalferoz@gmail.com">Faisal Feroz</a>
 */
public class DynamoFlexibleComparisonStrategy implements DynamoComparisonStrategy {

    private String[] ignorePropertyValues = new String[0];

    @Override
    public boolean compare(DynamoDbConnectionCallback connection, InputStream dataset) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

        Map<String, List<Map<String, AttributeValue>>> parsedData = objectMapper.readValue(dataset,
                ExpectedDataSet.TYPE_REFERENCE);

        DynamoDbAssertion.flexibleAssertEquals(new ExpectedDataSet(parsedData), ignorePropertyValues,
                connection.dbClient());

        return true;
    }

    @Override
    public void setIgnoreProperties(String[] ignorePropertyValues) {
        this.ignorePropertyValues = ignorePropertyValues;
    }
}
