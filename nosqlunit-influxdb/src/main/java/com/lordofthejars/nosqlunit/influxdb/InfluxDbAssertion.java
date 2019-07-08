
package com.lordofthejars.nosqlunit.influxdb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.influxdb.InfluxDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.FailureHandler;

public class InfluxDbAssertion {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDbAssertion.class);

    private InfluxDbAssertion() {
        super();
    }

    public static final void strictAssertEquals(final ExpectedDataSet expectedData, final InfluxDB influxDb) {
        final Set<String> measurementaNames = expectedData.getTables();
        final List<String> measurementNames = InfluxDBOperation.getAllMeasurements(influxDb);

        checkMeasurementName(measurementaNames, measurementNames);

        for (final String measurementName : measurementaNames) {
            checkMeasurementObjects(expectedData, influxDb, measurementName);
        }
    }

    private static void checkMeasurementName(final Set<String> expectedMeasurementNames,
            final List<String> influxdbMeasurementNames) {
        final Set<String> allMeasurements = new HashSet<>(influxdbMeasurementNames);
        allMeasurements.addAll(expectedMeasurementNames);

        if (allMeasurements.size() != expectedMeasurementNames.size()
                || allMeasurements.size() != influxdbMeasurementNames.size()) {
            throw FailureHandler.createFailure(
                    "Expected measurement names are %s but inserted measurement names are %s", expectedMeasurementNames,
                    influxdbMeasurementNames);
        }
    }

    private static void checkMeasurementObjects(final ExpectedDataSet expectedData, final InfluxDB influxDb,
            final String measurementName) {
        final List<ExpectedPoint> dataObjects = expectedData.getDataFor(measurementName);
        final List<ExpectedPoint> dbTable = InfluxDBOperation.getAllItems(influxDb, measurementName);

        final int expectedDataObjectsCount = dataObjects.size();
        final long insertedDataObjectsCount = dbTable.size();

        if (expectedDataObjectsCount != insertedDataObjectsCount) {
            throw FailureHandler.createFailure("Expected measurement has %s elements but inserted measurement has %s",
                    expectedDataObjectsCount, insertedDataObjectsCount);
        }

        for (final ExpectedPoint expectedDataObject : dataObjects) {
            if (!dbTable.contains(expectedDataObject)) {
                throw FailureHandler.createFailure("Object # %s # is not found into measurement [%s]",
                        expectedDataObject.toString(), measurementName);

            }
        }

    }

    /**
     * Checks that all the expected data is present in InfluxDB.
     *
     * @param expectedData Expected data.
     * @param influxDb Influx Database.
     */
    public static void flexibleAssertEquals(final ExpectedDataSet expectedData, final String[] ignorePropertyValues,
            final InfluxDB influxDb) {
        // Get the expected measurements
        final Set<String> measurementNames = expectedData.getTables();

        // Get the current measurements in influxDB
        final List<String> listTableNames = InfluxDBOperation.getAllMeasurements(influxDb);

        // Get the concrete property names that should be ignored
        // Map<String:Table, Set<String:Property>>
        final Map<String, Set<String>> propertiesToIgnore = parseIgnorePropertyValues(measurementNames,
                ignorePropertyValues);

        // Check expected data
        flexibleCheckTablesName(measurementNames, listTableNames);
        for (final String measurementName : measurementNames) {
            flexibleCheckTableObjects(expectedData, influxDb, measurementName, propertiesToIgnore);
        }
    }

    /**
     * Resolve the properties that will be ignored for each expected measurement.
     * <p/>
     *
     * @param ignorePropertyValues Input values defined with @IgnorePropertyValue.
     * @return Map with the properties that will be ignored for each document.
     */
    private static Map<String, Set<String>> parseIgnorePropertyValues(final Set<String> measurementNames,
            final String[] ignorePropertyValues) {
        final Map<String, Set<String>> propertiesToIgnore = new HashMap<>();
        final Pattern measurementAndPropertyPattern = Pattern.compile(
                "^(.*?)([.])(.*)$");
        final Pattern propertyPattern = Pattern.compile("^([^$][^.0]*)$");

        for (final String ignorePropertyValue : ignorePropertyValues) {
            final Matcher measurementAndPropertyMatcher = measurementAndPropertyPattern.matcher(ignorePropertyValue);
            final Matcher propertyMatcher = propertyPattern.matcher(ignorePropertyValue);

            // If the property to ignore includes the measurement, add it to only exclude
            // the property in the indicated measurement
            if (measurementAndPropertyMatcher.matches()) {
                // Add the property to ignore to the proper measurement
                final String measurementName = measurementAndPropertyMatcher.group(1);
                final String propertyName = measurementAndPropertyMatcher.group(3);

                if (measurementNames.contains(measurementName)) {
                    Set<String> properties = propertiesToIgnore.get(measurementName);
                    if (properties == null) {
                        properties = new HashSet<>();
                    }
                    properties.add(propertyName);
                    propertiesToIgnore.put(measurementName, properties);
                } else {
                    LOGGER.warn("Measurement {} for {} is not defined as expected. It won't be used for ignoring properties",
                            measurementName, ignorePropertyValue);
                }
                // If the property to ignore doesn't include the measurement, add it to
                // all the expected measurements
            } else if (propertyMatcher.matches()) {
                final String propertyName = propertyMatcher.group(0);

                // Add the property to ignore to all the expected measurements
                for (final String measurementName : measurementNames) {
                    Set<String> properties = propertiesToIgnore.get(measurementName);
                    if (properties == null) {
                        properties = new HashSet<>();
                    }
                    properties.add(propertyName);
                    propertiesToIgnore.put(measurementName, properties);
                }
                // If doesn't match any pattern
            } else {
                LOGGER.warn(
                        "Property {} has an invalid measurement.property value. It won't be used for ignoring properties",
                        ignorePropertyValue);
            }
        }

        return propertiesToIgnore;
    }

    /**
     * Checks that all the expected measurement names are present in InfluxDB. Does not
     * check that all the measurement names present in Influx are in the expected dataset
     * measurement names.
     * <p/>
     * If any expected measurement isn't found in the database measurement, the returned
     * error indicates only the missing expected measurements.
     *
     * @param expectedTableNames Expected measurement names.
     * @param influxdbTableNames Current InfluxDB measurement names.
     */
    private static void flexibleCheckTablesName(final Set<String> expectedTableNames,
            final List<String> influxdbTableNames) {
        boolean ok = true;
        final HashSet<String> notFoundTableNames = new HashSet<>();
        for (final String expectedTableName : expectedTableNames) {
            if (!influxdbTableNames.contains(expectedTableName)) {
                ok = false;
                notFoundTableNames.add(expectedTableName);
            }
        }

        if (!ok) {
            throw FailureHandler.createFailure(
                    "The following measurement names %s were not found in the inserted measurement names",
                    notFoundTableNames);
        }
    }

    /**
     * Checks that each expected object in the measurement exists in the database.
     *
     * @param expectedData Expected data.
     * @param influxDb influx database.
     * @param measurementName Table name.
     */
    private static void flexibleCheckTableObjects(final ExpectedDataSet expectedData, final InfluxDB influxDb,
            final String measurementName, final Map<String, Set<String>> propertiesToIgnore) {
        final List<ExpectedPoint> dataObjects = expectedData.getDataFor(measurementName);

        final List<ExpectedPoint> dbTable = InfluxDBOperation.getAllItems(influxDb, measurementName);

        for (final ExpectedPoint expectedDataObject : dataObjects) {
            final ExpectedPoint filteredExpectedDataObject = filterProperties(expectedDataObject,
                    propertiesToIgnore.get(measurementName));
            final List<ExpectedPoint> foundObjects = dbTable.stream() //
                    .map(foundDataObject -> filterProperties(foundDataObject, propertiesToIgnore.get(measurementName))) //
                    .filter(map -> map.equals(filteredExpectedDataObject)) //
                    .collect(Collectors.toList());

            if (foundObjects.size() > 1) {
                LOGGER.warn(
                        "There were found {} possible matches for this object # {} #. That could have been caused by ignoring too many properties.",
                        foundObjects.size(), expectedDataObject);
            }

            if (foundObjects.isEmpty()) {
                throw FailureHandler.createFailure("Object # %s # is not found into measurement [%s]",
                        filteredExpectedDataObject.toString(), measurementName);
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
    @SuppressWarnings("rawtypes")
    private static ExpectedPoint filterProperties(final ExpectedPoint dataObject,
            final Set<String> propertiesToIgnore) {
        final ExpectedPoint filteredDataObject = new ExpectedPoint();
        filteredDataObject.putAll(dataObject);

        if (propertiesToIgnore == null) {
            return filteredDataObject;
        }

        for (final String property : propertiesToIgnore) {
            final String[] subs = property.split("[.]");
            if (subs.length > 1) {
                Map obj = filteredDataObject;
                for (int i = 0; i < subs.length - 1; i++) {
                    if (obj instanceof Map) {
                        obj = (Map) obj.get(subs[i]);
                    }
                }
                obj.remove(subs[subs.length - 1]);
            } else {
                filteredDataObject.remove(subs[0]);
            }

        }

        return filteredDataObject;
    }

}
