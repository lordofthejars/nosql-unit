
package com.lordofthejars.nosqlunit.influxdb;

import static com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDbConfigurationBuilder.*;
import static com.lordofthejars.nosqlunit.influxdb.InfluxDbConfigurationBuilder.*;

import org.influxdb.InfluxDB;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class InfluxDbRule extends AbstractNoSqlTestRule {

    private static final String EXTENSION = "json";

    protected DatabaseOperation<InfluxDB> databaseOperation;

    public static class InfluxDbRuleBuilder {

        private InfluxDbConfiguration influxDbConfiguration;

        private Object target;

        private InfluxDbRuleBuilder() {
        }

        public static InfluxDbRuleBuilder newInfluxDbRule() {
            return new InfluxDbRuleBuilder();
        }

        public InfluxDbRuleBuilder configure(final InfluxDbConfiguration influxDbConfiguration) {
            this.influxDbConfiguration = influxDbConfiguration;
            return this;
        }

        public InfluxDbRuleBuilder unitInstance(final Object target) {
            this.target = target;
            return this;
        }

        public InfluxDbRule defaultEmbeddedInfluxDb(final String databaseName) {
            return new InfluxDbRule(inMemoryInfluxDb().databaseName(databaseName).build());
        }

        public InfluxDbRule defaultSpringInfluxDb() {
            return new SpringInfluxDbRule(influxDb().build());
        }

        public InfluxDbRule build() {

            if (this.influxDbConfiguration == null) {
                throw new IllegalArgumentException("Configuration object should be provided.");
            }

            return new InfluxDbRule(influxDbConfiguration, target);
        }

    }

    public InfluxDbRule(final InfluxDbConfiguration influxDbConfiguration) {
        super(influxDbConfiguration.getConnectionIdentifier());
        databaseOperation = new InfluxOperation(influxDbConfiguration);
    }

    /*
     * With JUnit 10 is impossible to get target from a Rule, it seems that future
     * versions will support it. For now constructor is approach is the only way.
     */
    public InfluxDbRule(final InfluxDbConfiguration influxDbConfiguration, final Object target) {
        super(influxDbConfiguration.getConnectionIdentifier());
        setTarget(target);
        databaseOperation = new InfluxOperation(influxDbConfiguration);
    }

    @Override
    public DatabaseOperation<InfluxDB> getDatabaseOperation() {
        return this.databaseOperation;
    }

    @Override
    public String getWorkingExtension() {
        return EXTENSION;
    }

    @Override
    public void close() {
        // do nothing
    }

}
