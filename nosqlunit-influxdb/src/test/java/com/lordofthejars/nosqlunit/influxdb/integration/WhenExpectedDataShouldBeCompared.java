
package com.lordofthejars.nosqlunit.influxdb.integration;

import static com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDb.InMemoryInfluxRuleBuilder.*;
import static com.lordofthejars.nosqlunit.influxdb.InfluxDbConfigurationBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDb;
import com.lordofthejars.nosqlunit.influxdb.InfluxDBOperation;
import com.lordofthejars.nosqlunit.influxdb.InfluxDbConfiguration;
import com.lordofthejars.nosqlunit.influxdb.InfluxOperation;

public class WhenExpectedDataShouldBeCompared {

    private static final String DB_NAME = "test-db";

    private static final long TIME = 1514764800000000000L;

    @ClassRule
    public static final InMemoryInfluxDb IN_MEMORY_INFLUX_DB = newInMemoryInfluxDbRule().build();

    private static InfluxOperation influxOperation;

    @BeforeClass
    public static final void startUp() {
        final InfluxDbConfiguration influxConfiguration = influxDb().databaseName(DB_NAME).build();
        influxOperation = new InfluxOperation(influxConfiguration);
    }

    @Before
    public void setUp() {
        createDatabase(getInfluxDB(), DB_NAME);
        dropAllMeasurement(getInfluxDB());
    }

    @Test
    public void empty_database_and_empty_expectation_should_be_equals() {
        final boolean isEquals = influxOperation.databaseIs(new ByteArrayInputStream("{}".getBytes()));

        assertThat(isEquals, is(true));
    }

    @Test
    public void empty_expected_measurement_and_database_measurement_with_content_should_fail() throws Exception {
        try {
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

            influxOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[]}".getBytes("UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected measurement has 0 elements but inserted measurement has 1"));
        }
    }

    @Test
    public void empty_expected_measurement_and_empty_database_should_fail() throws Exception {
        try {
            influxOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[]}".getBytes("UTF-8")));

            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Expected measurement names are [col1] but inserted measurement names are []"));
        }

    }

    @Test
    public void empty_expectation_and_empty_database_measurement_should_fail() throws Exception {
        try {
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

            influxOperation.databaseIs(new ByteArrayInputStream("{}".getBytes("UTF-8")));

            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Expected measurement names are [] but inserted measurement names are [col1]"));
        }

    }

    @Test
    public void expected_measurement_and_database_measurement_with_same_content_should_be_equals() throws Exception {
        addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

        final boolean isEquals = influxOperation.databaseIs(new ByteArrayInputStream(
                "{\"col1\":[ {\"time\": 1514764800000000000, \"precision\": \"NANOSECONDS\", \"fields\": {\"name\":\"Alex\"} }]}".getBytes(
                        "UTF-8")));
        assertThat(isEquals, is(true));

    }

    @Test
    public void expected_measurement_and_database_measurement_with_different_content_should_fail() throws Exception {
        try {
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

            influxOperation.databaseIs(
                    new ByteArrayInputStream("{\"col1\":[{\"fields\": {\"name\":\"Soto\"} }]}".getBytes("UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Object # {{fields={name=Soto}, measurement=col1}} # is not found into measurement [col1]"));
        }

    }

    @Test
    public void expected_measurement_with_content_and_database_measurement_empty_should_fail() throws Exception {

        try {
            influxOperation.databaseIs(
                    new ByteArrayInputStream("{\"col1\":[{\"fields\": {\"name\":\"Alex\"}}]}".getBytes("UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Expected measurement names are [col1] but inserted measurement names are []"));
        }

    }

    @Test
    public void expected_measurement_and_database_measurement_with_different_names_should_fail() throws Exception {
        try {
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

            influxOperation.databaseIs(
                    new ByteArrayInputStream("{\"col2\":[{\"fields\": {\"name\":\"Alex\"}}]}".getBytes("UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Expected measurement names are [col2] but inserted measurement names are [col1]"));
        }

    }

    @Test
    public void less_expected_measurement_than_database_measurement_should_fail() throws Exception {
        try {
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");
            addMeasurementWithData(getInfluxDB(), "col3", "name", "Alex");

            influxOperation.databaseIs(
                    new ByteArrayInputStream("{\"col1\":[{\"fields\": {\"name\":\"Alex\"}}]}".getBytes("UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Expected measurement names are [col1] but inserted measurement names are [col1, col3]"));
        }

    }

    @Test
    public void expected_measurement_has_some_items_different_than_database_measurement_items_should_fail()
            throws Exception {
        try {
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

            influxOperation.databaseIs(new ByteArrayInputStream(
                    "{\"col1\":[{\"fields\": {\"name\":\"Alex\"}}, {\"fields\": {\"name\":\"Soto\"}}]}".getBytes(
                            "UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected measurement has 2 elements but inserted measurement has 1"));
        }

    }

    @Test
    public void expected_measurement_item_has_more_attributes_than_database_measurement_item_attributes_should_fail()
            throws Exception {
        try {
            // Inserted one element
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

            // Expected with two elements
            influxOperation.databaseIs(new ByteArrayInputStream(
                    "{\"col1\":[{\"fields\": {\"name\":\"Alex\", \"surname\":\"Soto\"}}]}".getBytes("UTF-8")));
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(), is(
                    "Object # {{fields={name=Alex, surname=Soto}, measurement=col1}} # is not found into measurement [col1]"));
        }

    }

    @Test
    public void expected_measurement_item_has_same_attributes_as_database_measurement_item_attributes_but_different_values_should_fail()
            throws Exception {
        try {
            // Inserted one element
            addMeasurementWithTwoData(getInfluxDB(), "col1", "name", "Alex", "surname", "Sot");

            // Expected with two elements
            influxOperation.databaseIs(new ByteArrayInputStream(
                    "{\"col1\":[{\"fields\": {\"name\":\"Alex\", \"surname\":\"Soto\"}}]}".getBytes("UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected measurement has 1 elements but inserted measurement has 2"));
        }

    }

    @Test
    public void expected_measurement_item_has_less_attributes_than_database_measurement_item_attributes_should_fail()
            throws Exception {
        try {
            // Inserted one element
            addMeasurementWithTwoData(getInfluxDB(), "col1", "name", "Alex", "surname", "Soto");

            // Expected with two elements
            influxOperation.databaseIs(
                    new ByteArrayInputStream("{\"col1\":[{\"fields\": {\"name\":\"Alex\"}}]}".getBytes("UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(), is("Expected measurement has 1 elements but inserted measurement has 2"));
        }

    }

    @Test
    public void expected_measurement_has_all_items_different_than_database_measurement_items_should_fail()
            throws Exception {
        try {
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

            influxOperation.databaseIs(
                    new ByteArrayInputStream("{\"col1\":[{\"fields\": {\"name\":\"Soto\"}}]}".getBytes("UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Object # {{fields={name=Soto}, measurement=col1}} # is not found into measurement [col1]"));
        }

    }

    @Test
    public void more_expected_measurement_than_database_measurement_should_fail() throws Exception {
        try {
            addMeasurementWithData(getInfluxDB(), "col1", "name", "Alex");

            influxOperation.databaseIs(new ByteArrayInputStream(
                    "{\"col1\":[{\"fields\": {\"name\":\"Alex\"}}], \"col3\":[{\"fields\": {\"name\":\"Alex\"}}]}".getBytes(
                            "UTF-8")));
            fail();
        } catch (final NoSqlAssertionError e) {
            assertThat(e.getMessage(),
                    is("Expected measurement names are [col1, col3] but inserted measurement names are [col1]"));
        }

    }

    private void addMeasurementWithTwoData(final InfluxDB influxDb, final String measurementName, final String field,
            final String value, final String field2, final String value2) throws InterruptedException {
        final Point point1 = Point.measurement(measurementName).time(TIME, TimeUnit.NANOSECONDS).addField(field,
                value).build();
        final Point point2 = Point.measurement(measurementName).time(TIME + 1, TimeUnit.NANOSECONDS).addField(field2,
                value2).build();

        influxDb.write(point1);
        influxDb.write(point2);
    }

    private void addMeasurementWithData(final InfluxDB influxDb, final String measurementName, final String field,
            final String value) throws InterruptedException {
        final Point point = Point.measurement(measurementName).time(TIME, TimeUnit.NANOSECONDS).addField(field,
                value).build();
        influxDb.write(point);
    }

    private void dropAllMeasurement(final InfluxDB influxDb) {
        final List<String> allMeasurementNames = InfluxDBOperation.getAllMeasurements(influxDb);
        InfluxDBOperation.deleteMeasurements(influxDb, allMeasurementNames);
    }

    @SuppressWarnings("deprecation")
    private void createDatabase(final InfluxDB influxDb, final String dbName) {
        influxDb.createDatabase(dbName);
    }

    private InfluxDB getInfluxDB() {
        return influxOperation.connectionManager();
    }

}
