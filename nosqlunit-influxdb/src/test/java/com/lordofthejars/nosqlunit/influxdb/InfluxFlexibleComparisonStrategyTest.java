
package com.lordofthejars.nosqlunit.influxdb;

import static com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDb.InMemoryInfluxRuleBuilder.*;
import static com.lordofthejars.nosqlunit.influxdb.InfluxDbRule.InfluxDbRuleBuilder.*;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.CustomComparisonStrategy;
import com.lordofthejars.nosqlunit.annotation.IgnorePropertyValue;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;

@CustomComparisonStrategy(comparisonStrategy = InfluxFlexibleComparisonStrategy.class)
public class InfluxFlexibleComparisonStrategyTest {

    private static final String DB_URL = "http://localhost:8086";

    private static final String TEST_DB = "test-db";

    @ClassRule
    public static final InMemoryInfluxDb IN_MEMORY_INFLUX_DB = newInMemoryInfluxDbRule().build();

    @Rule
    public InfluxDbRule dynamoDbRule = newInfluxDbRule().defaultEmbeddedInfluxDb(TEST_DB);

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

    @Test
    @UsingDataSet(locations = "InfluxFlexibleComparisonStrategyTest#thatShowWarnings.json")
    @ShouldMatchDataSet(location = "InfluxFlexibleComparisonStrategyTest#thatShowWarnings-expected.json")
    @IgnorePropertyValue(properties = { "time", "measurement1.fields.tag1" })
    public void shouldIgnorePropertiesInFlexibleStrategy() {
    }
}
