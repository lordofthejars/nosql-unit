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
 *      <ul>
 *          Checks that all the expected collections are present in Mongo DB, but accepts
 *          other collections stored in the database that are not defined in the expected
 *          file.
 *      </ul>
 *      <ul>
 *          Checks that all the expected objects are present in Mongo DB, but accepts
 *          other objects stored in the same collections that are not defined as expected.
 *      </ul>
 *      <ul>
 *          For each object checks that all properties set to be ignored its value
 *          exist in the object stored in the database, but it accepts any saved value.
 *      </ul>
 * </li>
 *
 * The annotation '@IgnorePropertyValue(properties = {String...})' allows the user define
 * the properties that should be ignored when checking the expected objects.
 *
 * It accepts two formats for property definition:
 * <li>
 *     <ol>
 *         collection.property : When are defined both collection and property name
 *         the exclusion will only affect to the indicated collection.
 *         e.g: With @IgnorePropertyValue(properties = {"book.date"}), the property date
 *         will be ignored in each object of the 'book' collection. If other objects in
 *         different collections have the property 'date' it won't be ignored.
 *     </ol>
 *     <ol>
 *         property : When only is defined the property name it will be excluded
 *         from all objects in any expected collection.
 *         e.g: With @IgnorePropertyValue(properties = {"date"}), the property 'date'
 *         will be ignored in each object, no matter the collection.
 *     </ol>
 * </li>
 *
 * The values of the properties to be ignored should be named following the rules for valid
 * collection and property names defined in
 * <a href="http://docs.mongodb.org/manual/reference/limits/#naming-restrictions>
 * "Mongo DB: naming restrictions"</a> document.
 *
 * When more than one object stored in the database matches the expected object after
 * ignoring properties a warning is shown notifying the number of objects that were found.
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
