
package com.lordofthejars.nosqlunit.influxdb;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class utilities for managing database. I contains all methods to get, create and delete
 * measurements thereby to insert data.
 *
 * @author Faisal Feroz
 *
 */
public abstract class InfluxDBOperation {

    private InfluxDBOperation() {
    }

    /**
     * {@link InfluxDBOperation}'s Logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBOperation.class);

    /**
     * Returns all existing measurements in database.
     *
     * @param client client used to connect to database.
     * @return list of measurement name.
     */
    public static List<String> getAllMeasurements(final InfluxDB client) {
        return client.query(new Query("SHOW measurements")) //
                .getResults() //
                .stream() //
                .filter(result -> !result.hasError() && result.getSeries() != null) //
                .flatMap(result -> result.getSeries().stream()) //
                .flatMap(series -> series.getValues().stream()) //
                .flatMap(List<Object>::stream) //
                .map(String::valueOf) //
                .collect(Collectors.toList());
    }

    /**
     * Deletes measurements whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param names list of measurement name
     * @return true if all measurements are deleted
     */
    public static boolean deleteMeasurements(final InfluxDB client, final List<String> names) {
        boolean result = true;
        for (final String name : names) {
            result = result && deleteMeasurement(client, name);
        }
        return result;
    }

    /**
     * Deletes measurement whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param name list of measurement name
     * @return true if measurement is deleted
     */
    public static boolean deleteMeasurement(final InfluxDB client, final String name) {
        return !client.query(new Query("DROP MEASUREMENT " + name)).hasError();
    }

    /**
     * Inserts data received in the <code>dataSetResourceFile</code>.
     *
     * @param client client used to connect to database
     * @param dataSetResourceFile data set resource file
     * @return true if all items are inserted.
     * @throws IOException thrown when error occurred while trying to map data found in
     *         <code>is</code> to influxdb object
     */
    public static boolean insertData(final InfluxDB client, final InputStream dataSetResourceFile) throws IOException {
        if (dataSetResourceFile == null) {
            LOGGER.error("data set file cannot be null");
            throw new IllegalArgumentException("data set file cannot be null");
        }
        boolean result = true;

        final ObjectMapper objectMapper = new ObjectMapper();

        final Map<String, List<ExpectedPoint>> parsedData = objectMapper.readValue(dataSetResourceFile,
                ExpectedDataSet.TYPE_REFERENCE);

        final List<ExpectedPoint> items = parsedData.entrySet() //
                .stream() //
                .flatMap((final Entry<String, List<ExpectedPoint>> entry) -> {
                    final String measurement = entry.getKey();
                    return entry.getValue().stream().map(point -> {
                        point.setMeasurement(measurement);
                        return point;
                    });
                }) //
                .collect(Collectors.toList());

        for (final ExpectedPoint point : items) {
            result = result && insertItem(client, point);
        }
        return result;
    }

    /**
     * Inserts point received as parameter.
     *
     * @param client client used to connect to database
     * @param point point to insert
     * @return true if point is inserted.
     */
    public static boolean insertItem(final InfluxDB client, final ExpectedPoint point) {
        try {
            client.write(point.toPoint());
            return true;
        } catch (final Exception e) {
            LOGGER.error("Error occurred when trying to insert [ " + point + " ]", e);
            return false;
        }
    }

    /**
     * Get all items from measurement whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param name measurement name
     * @return A list of Points.
     */
    public static List<ExpectedPoint> getAllItems(final InfluxDB client, final String name) {
        LOGGER.debug("starting getAllItems method...");
        return client.query(new Query("SELECT * FROM " + name)) //
                .getResults() //
                .stream() //
                .filter(result -> !result.hasError() && result.getSeries() != null) //
                .flatMap(result -> result.getSeries().stream()) //
                .flatMap(series -> series.getValues().stream().map(value -> {
                    final Map<String, Object> fields = new HashMap<>();
                    final Instant instant = Instant.parse((String) value.get(0));
                    final long time = TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();

                    for (int i = 1; i < series.getColumns().size(); i++) {
                        fields.put(series.getColumns().get(i), value.get(i));
                    }

                    final ExpectedPoint point = new ExpectedPoint();
                    point.setTime(time);
                    point.setPrecision(TimeUnit.NANOSECONDS);
                    point.setMeasurement(series.getName());
                    point.setFields(fields);
                    return point;
                })) //
                .collect(Collectors.toList());

    }

    /**
     * Truncates the measurements whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param names list of measurement name
     * @return true if all measurements are truncated
     */
    public static boolean truncateMeasurements(final InfluxDB client, final List<String> names) {
        LOGGER.debug("starting truncateMeasurements method...");
        boolean result = true;
        for (final String measurementName : names) {
            result = result && truncateMeasurement(client, measurementName);
        }
        return result;
    }

    /**
     * Truncates measurement whose name is received as parameter.
     *
     * @param client client used to connect to database
     * @param measurement measurement name
     * @return true if measurement is deleted
     */
    public static boolean truncateMeasurement(final InfluxDB client, final String measurement) {
        LOGGER.debug("starting truncateMeasurement method...");
        boolean result = true;
        try {
            for (final ExpectedPoint point : getAllItems(client, measurement)) {
                result = result && deleteItem(client, point);
            }
            return result;
        } catch (final Exception e) {
            LOGGER.error("Error occurred when trying to truncate measurement [ " + measurement + " ]", e);
            return false;
        }
    }

    /**
     * Deletes item received as parameter.
     *
     * @param client client used to connect to database
     * @param point point to delete
     * @return true if item is deleted.
     */
    public static boolean deleteItem(final InfluxDB client, final ExpectedPoint point) {
        try {
            return !client.query(
                    new Query("DELETE FROM " + point.getMeasurement() + " WHERE time = " + point.getTime())) //
                    .hasError();
        } catch (final Exception e) {
            LOGGER.error("Error occurred when trying to delete point [ " + point.getTime() + " ] in measurement [ "
                    + point.getMeasurement() + " ]", e);
            return false;
        }

    }

}
