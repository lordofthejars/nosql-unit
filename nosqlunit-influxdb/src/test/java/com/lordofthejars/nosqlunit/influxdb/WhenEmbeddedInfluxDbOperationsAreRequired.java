
package com.lordofthejars.nosqlunit.influxdb;

import static com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDb.InMemoryInfluxRuleBuilder.*;
import static com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDbConfigurationBuilder.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class WhenEmbeddedInfluxDbOperationsAreRequired {

    private static final String DB_URL = "http://localhost:8086";

    private static final String TEST_DB = "test-db";

    private static final String DATA = "{\"measurement1\": [" +
            "        {" +
            "            \"tags\": {" +
            "                \"tag1\": \"value0\"" +
            "            }," +
            "            \"time\": 1514764800000000000," +
            "            \"precision\": \"NANOSECONDS\"," +
            "            \"fields\": {" +
            "                \"field1\": \"field value01\"," +
            "                \"field2\": \"field value02\"" +
            "            }" +
            "        }" +
            "    ]}";

    private static final String COMPARE_DATA = "{\"measurement1\": [" +
            "        {" +
            "            \"time\": 1514764800000000000," +
            "            \"precision\": \"NANOSECONDS\"," +
            "            \"fields\": {" +
            "                \"field1\": \"field value01\"," +
            "                \"field2\": \"field value02\"," +
            "                \"tag1\": \"value0\"" +
            "            }" +
            "        }" +
            "    ]}";

    @ClassRule
    public static final InMemoryInfluxDb IN_MEMORY_DYNAMO_DB = newInMemoryInfluxDbRule().build();

    @SuppressWarnings("deprecation")
    @BeforeClass
    public static void setup() throws Exception {
        final InfluxDB influxDb = InfluxDBFactory.connect(DB_URL);
        influxDb.createDatabase(TEST_DB);
    }

    @SuppressWarnings("deprecation")
    @AfterClass
    public static void after() {
        final InfluxDB influxDb = InfluxDBFactory.connect(DB_URL);
        influxDb.deleteDatabase(TEST_DB);
    }

    @After
    public void tearDown() {
        final InfluxDB defaultEmbeddedInstance = EmbeddedInfluxInstancesFactory.getInstance().getDefaultEmbeddedInstance();
        InfluxDBOperation.truncateMeasurement(defaultEmbeddedInstance, "measurement1");
    }

    @Test
    public void data_should_be_inserted_into_influxdb() {

        final InfluxOperation influxOperation = new InfluxOperation(inMemoryInfluxDb().databaseName(TEST_DB).build());
        influxOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        final InfluxDB influx = influxOperation.connectionManager();
        final List<ExpectedPoint> allItems = InfluxDBOperation.getAllItems(influx, "measurement1");

        assertThat(allItems, hasSize(1));

        final ExpectedPoint object = allItems.get(0);

        assertThat(object.getTime(), is(notNullValue()));
        assertThat(object.getTime(), is(1514764800000000000L));

        assertThat(object.getPrecision(), is(TimeUnit.NANOSECONDS));

        assertThat(object.getFields(), hasEntry("field1", "field value01"));
        assertThat(object.getFields(), hasEntry("field2", "field value02"));
        assertThat(object.getFields(), hasEntry("tag1", "value0"));
    }

    @Test
    public void data_should_be_removed_from_influxdb() {

        final InfluxOperation influxOperation = new InfluxOperation(inMemoryInfluxDb().databaseName(TEST_DB).build());
        influxOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        influxOperation.deleteAll();

        final InfluxDB influx = influxOperation.connectionManager();
        final List<ExpectedPoint> allItems = InfluxDBOperation.getAllItems(influx, "measurement1");

        assertThat(allItems, hasSize(0));
    }

    @Test
    public void data_should_be_compared_between_expected_and_current_data() {

        final InfluxOperation influxOperation = new InfluxOperation(inMemoryInfluxDb().databaseName(TEST_DB).build());
        influxOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        final boolean result = influxOperation.databaseIs(new ByteArrayInputStream(COMPARE_DATA.getBytes()));

        assertThat(result, is(true));
    }

}
