
package com.lordofthejars.nosqlunit.influxdb;

import org.influxdb.InfluxDB;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.springframework.context.ApplicationContext;

import com.lordofthejars.nosqlunit.core.PropertyGetter;
import com.lordofthejars.nosqlunit.util.SpringUtils;

public class SpringInfluxDbRule extends InfluxDbRule {

    private final PropertyGetter<ApplicationContext> propertyGetter = new PropertyGetter<>();

    public SpringInfluxDbRule(final InfluxDbConfiguration influxDbConfiguration) {
        super(influxDbConfiguration);
    }

    public SpringInfluxDbRule(final InfluxDbConfiguration influxDbConfiguration, final Object object) {
        super(influxDbConfiguration, object);
    }

    @Override
    public Statement apply(final Statement base, final FrameworkMethod method, final Object testObject) {
        this.databaseOperation = new InfluxOperation(definedInflux(testObject));
        return super.apply(base, method, testObject);
    }

    @Override
    public void close() {
        // DO NOT CLOSE the connection (Spring will do it when destroying the context)
    }

    private InfluxDB definedInflux(final Object testObject) {
        final ApplicationContext applicationContext = propertyGetter.propertyByType(testObject,
                ApplicationContext.class);

        final InfluxDB influx = SpringUtils.getBeanOfType(applicationContext, InfluxDB.class);

        if (influx == null) {
            throw new IllegalArgumentException(
                    "At least one InfluxDB instance should be defined into Spring Application Context.");
        }
        return influx;
    }

}
