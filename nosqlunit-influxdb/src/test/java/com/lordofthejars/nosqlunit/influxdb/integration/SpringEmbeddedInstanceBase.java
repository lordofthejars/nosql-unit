
package com.lordofthejars.nosqlunit.influxdb.integration;

import static com.lordofthejars.nosqlunit.influxdb.InfluxDbRule.InfluxDbRuleBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.influxdb.InfluxDB;
import org.junit.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.influxdb.InfluxDbRule;

public abstract class SpringEmbeddedInstanceBase {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private InfluxDB influxDb;

    @Rule
    public InfluxDbRule influxDbRule = newInfluxDbRule().defaultSpringInfluxDb();

    protected void validateInfluxConnection() {
        DatabaseOperation<InfluxDB> databaseOperation = influxDbRule.getDatabaseOperation();
        InfluxDB connectionManager = databaseOperation.connectionManager();

        assertThat(connectionManager, is(influxDb));
    }

}
