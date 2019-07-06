
package com.lordofthejars.nosqlunit.influxdb.integration;

import static com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDb.InMemoryInfluxRuleBuilder.*;
import static com.lordofthejars.nosqlunit.influxdb.InfluxDbConfigurationBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDb;
import com.lordofthejars.nosqlunit.influxdb.InfluxDBOperation;
import com.lordofthejars.nosqlunit.influxdb.InfluxDbConfiguration;
import com.lordofthejars.nosqlunit.influxdb.InfluxDbRule;

public class WhenInfluxDbRuleIsRegistered {

    private static final String DB_NAME = "test-db";

    @ClassRule
    public static final InMemoryInfluxDb IN_MEMORY_INFLUX_DB = newInMemoryInfluxDbRule().build();

    @SuppressWarnings("deprecation")
    @Before
    public void setup() {
        try (final InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086")) {
            influxDB.createDatabase(DB_NAME);
        }
    }

    @After
    public void teardown() {
        try (final InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086")) {
            influxDB.setDatabase(DB_NAME);
            dropAllMeasurements(influxDB);
        }
    }

    @Test(expected = NoSqlAssertionError.class)
    public void should_fail_if_expected_data_is_non_strict_equal() throws Throwable {

        final InfluxDbConfiguration influxDbConfiguration = influxDb().databaseName(DB_NAME).build();
        final InfluxDbRule remoteInfluxDbRule = new InfluxDbRule(influxDbConfiguration);

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_wrong_test");

        final Statement influxdbStatement = remoteInfluxDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
        influxdbStatement.evaluate();

    }

    @Test
    public void should_assert_if_expected_data_is_strict_equal() throws Throwable {

        final InfluxDbConfiguration influxDbConfiguration = influxDb().databaseName(DB_NAME).build();
        final InfluxDbRule remoteInfluxDbRule = new InfluxDbRule(influxDbConfiguration);

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_equal_test");

        final Statement influxdbStatement = remoteInfluxDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
        influxdbStatement.evaluate();

    }

    @Test
    public void should_clean_dataset_with_delete_all_strategy() throws Throwable {

        final InfluxDbConfiguration influxDbConfiguration = influxDb().databaseName(DB_NAME).build();
        final InfluxDbRule remoteInfluxDbRule = new InfluxDbRule(influxDbConfiguration);

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_delete_test");

        final Statement influxdbStatement = remoteInfluxDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
        influxdbStatement.evaluate();

        final Map<String, Object> currentData = findItemByKey("measurement1", "time", 1514764800000000001L);
        assertThat(currentData, nullValue());
    }

    @Test
    public void should_insert_new_dataset_with_insert_strategy() throws Throwable {

        final InfluxDbConfiguration influxDbConfiguration = influxDb().databaseName(DB_NAME).build();
        final InfluxDbRule remoteInfluxDbRule = new InfluxDbRule(influxDbConfiguration);

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_insert_test_1");

        final MyTestClass testObject = new MyTestClass();
        final Statement influxdbStatement = remoteInfluxDbRule.apply(noStatement, frameworkMethod, testObject);
        influxdbStatement.evaluate();

        final Map<String, Object> currentData = findItemByKey("measurement1", "time", 1514764800000000001L);
        assertThat(currentData, is(notNullValue()));
        assertThat(currentData.get("code"), is(notNullValue()));
        assertThat(currentData.get("code"), is("JSON dataset"));

        final FrameworkMethod frameworkMethod2 = frameworkMethod(MyTestClass.class, "my_insert_test_2");

        final Statement influxdbStatement2 = remoteInfluxDbRule.apply(noStatement, frameworkMethod2, testObject);
        influxdbStatement2.evaluate();

        final Map<String, Object> previousData = findItemByKey("measurement1", "time", 1514764800000000001L);
        assertThat(previousData, is(notNullValue()));
        assertThat(previousData.get("code"), is(notNullValue()));
        assertThat(previousData.get("code"), is("JSON dataset"));

        final Map<String, Object> data = findItemByKey("measurement3", "time", 1514764800000000006L);
        assertThat(data, is(notNullValue()));
        assertThat(data.get("code"), is(notNullValue()));
        assertThat(data.get("code"), is("Another row"));

    }

    @Test
    public void should_clean_previous_data_and_insert_new_dataset_with_clean_insert_strategy() throws Throwable {

        final InfluxDbConfiguration influxDbConfiguration = influxDb().databaseName(DB_NAME).build();
        final InfluxDbRule remoteInfluxDbRule = new InfluxDbRule(influxDbConfiguration);

        final Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        final FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_equal_test");

        final MyTestClass testObject = new MyTestClass();

        final Statement influxdbStatement = remoteInfluxDbRule.apply(noStatement, frameworkMethod, testObject);
        influxdbStatement.evaluate();

        final Map<String, Object> currentData = findItemByKey("measurement1", "time", 1514764800000000001L);
        assertThat(currentData, is(notNullValue()));
        assertThat(currentData.get("code"), is(notNullValue()));
        assertThat(currentData.get("code"), is("JSON dataset"));

        final FrameworkMethod frameworkMethod2 = frameworkMethod(MyTestClass.class, "my_equal_test_2");

        final Statement influxdbStatement2 = remoteInfluxDbRule.apply(noStatement, frameworkMethod2, testObject);
        influxdbStatement2.evaluate();

        final Map<String, Object> previousData = findItemByKey("measurement1", "time", 1514764800000000001L);
        assertThat(previousData, nullValue());

        final Map<String, Object> data = findItemByKey("measurement3", "time", 1514764800000000006L);
        assertThat(data, is(notNullValue()));
        assertThat(data.get("code"), is(notNullValue()));
        assertThat(data.get("code"), is("Another row"));
    }

    private Map<String, Object> findItemByKey(final String measurementName, final String parameterName,
            final Number value) {
        try (final InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086")) {
            influxDB.setDatabase(DB_NAME);
            final QueryResult queryResult = influxDB.query(
                    new Query("SELECT * FROM " + measurementName + " WHERE " + parameterName + " = " + value));
            if (!queryResult.hasError() && queryResult.getResults() != null) {
                List<Map<String, Object>> items = queryResult.getResults() //
                        .stream() //
                        .filter(result -> !result.hasError() && result.getSeries() != null) //
                        .flatMap(result -> result.getSeries().stream()) //
                        .flatMap(series -> series.getValues().stream().map(val -> {
                            final Map<String, Object> map = new HashMap<>();
                            final List<String> columns = series.getColumns();
                            for (int i = 0; i < columns.size(); i++) {
                                final String col = columns.get(i);
                                map.put(col, val.get(i));
                            }
                            return map;
                        })) //
                        .collect(Collectors.toList());
                return items.size() == 1 ? items.get(0) : null;
            }
            return null;
        }
    }

    private FrameworkMethod frameworkMethod(final Class<?> testClass, final String methodName) {

        try {
            final Method method = testClass.getMethod(methodName);
            return new FrameworkMethod(method);
        } catch (final SecurityException e) {
            throw new IllegalArgumentException(e);
        } catch (final NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }

    }

    private void dropAllMeasurements(final InfluxDB influxDb) {
        final List<String> allMeasurementNames = InfluxDBOperation.getAllMeasurements(influxDb);
        InfluxDBOperation.deleteMeasurements(influxDb, allMeasurementNames);
    }
}

class MyTestClass {

    @Test
    @UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    @ShouldMatchDataSet(location = "json3.test")
    public void my_wrong_test() {
    }

    @Test
    @UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    @ShouldMatchDataSet(location = "json.test")
    public void my_equal_test() {
    }

    @Test
    @UsingDataSet(locations = "json2.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    public void my_equal_test_2() {
    }

    @Test
    @UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.DELETE_ALL)
    public void my_delete_test() {
    }

    @Test
    @UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.INSERT)
    public void my_insert_test_1() {
    }

    @Test
    @UsingDataSet(locations = "json2.test", loadStrategy = LoadStrategyEnum.INSERT)
    public void my_insert_test_2() {
    }

}
