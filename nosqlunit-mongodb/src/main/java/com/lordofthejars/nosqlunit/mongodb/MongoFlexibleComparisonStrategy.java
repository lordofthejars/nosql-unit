package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.FlexibleComparisonStrategy;
import com.lordofthejars.nosqlunit.core.IOUtils;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import java.io.IOException;
import java.io.InputStream;

/**
 * Comparison strategy that checks that all the expected data exists in the Mongo database.
 * It doesn't compare that all the data stored in the database is included in the expected
 * file, so other data not defined in the expected resource could exist in Mongo. It just
 * assure that the expected data exists.
 * <p/>
 * Checks the following assertions:
 * <li>
 * <ul>
 * Checks that all the expected collections are present in Mongo DB, but accepts other
 * collections stored in the database that are not defined in the expected file.
 * </ul>
 * <ul>
 * Checks that all the expected objects are present in Mongo DB, but accepts other
 * objects stored in the same collections that are not defined as expected.
 * </ul>
 * <ul>
 * For each object checks that all properties set with "@IgnorePropertyValue" value
 * exist in the object stored in the database, but it accepts any saved value.
 * </ul>
 * </li>
 *
 * @author <a mailto="victor.hernandezbermejo@gmail.com">Víctor Hernández</a>
 */
public class MongoFlexibleComparisonStrategy
        implements MongoComparisonStrategy, FlexibleComparisonStrategy {

    private String[] ignorePropertyValues = new String[0];

    @Override
    public boolean compare(MongoDbConnectionCallback connection, InputStream dataset) throws IOException {
        String expectedJsonData = loadContentFromInputStream(dataset);
        DBObject parsedData = parseData(expectedJsonData);

        MongoDbAssertion.flexibleAssertEquals(parsedData, ignorePropertyValues, connection.db());

        return true;
    }

    private String loadContentFromInputStream(InputStream inputStreamContent) throws IOException {
        return IOUtils.readFullStream(inputStreamContent);
    }

    private DBObject parseData(String jsonData) throws IOException {
        DBObject parsedData = (DBObject) JSON.parse(jsonData);
        return parsedData;
    }

    @Override
    public void setIgnorePropertyValues(String... ignorePropertyValues) {
        this.ignorePropertyValues = ignorePropertyValues;
    }
}
