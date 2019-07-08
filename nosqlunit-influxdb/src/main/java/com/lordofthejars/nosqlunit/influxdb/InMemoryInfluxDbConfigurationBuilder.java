package com.lordofthejars.nosqlunit.influxdb;
import org.influxdb.InfluxDB;

import com.lordofthejars.nosqlunit.core.FailureHandler;

public class InMemoryInfluxDbConfigurationBuilder {


    private final InfluxDbConfiguration influxDbConfiguration;

    public static InMemoryInfluxDbConfigurationBuilder inMemoryInfluxDb() {
        return new InMemoryInfluxDbConfigurationBuilder();
    }

    private InMemoryInfluxDbConfigurationBuilder() {
        this.influxDbConfiguration = new InfluxDbConfiguration();
    }

    public InMemoryInfluxDbConfigurationBuilder databaseName(final String databaseName) {
        this.influxDbConfiguration.setDatabaseName(databaseName);
        return this;
    }

    public InMemoryInfluxDbConfigurationBuilder connectionIdentifier(final String connectionIdentifier) {
        this.influxDbConfiguration.setConnectionIdentifier(connectionIdentifier);
        return this;
    }

    public InfluxDbConfiguration build() {

        final InfluxDB embeddedInflux = EmbeddedInfluxInstancesFactory.getInstance().getDefaultEmbeddedInstance();

        if(embeddedInflux == null) {
            throw FailureHandler.createIllegalStateFailure("There is no EmbeddedInflux rule with default target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
        }

        if(this.influxDbConfiguration.getDatabaseName() == null) {
            throw FailureHandler.createIllegalStateFailure("There is no database defined.");
        }

        embeddedInflux.setDatabase(this.influxDbConfiguration.getDatabaseName());

        this.influxDbConfiguration.setInflux(embeddedInflux);
        return this.influxDbConfiguration;

    }

    public InfluxDbConfiguration buildFromTargetPath(final String targetPath) {

        final InfluxDB embeddedInflux = EmbeddedInfluxInstancesFactory.getInstance().getEmbeddedByTargetPath(targetPath);

        if(embeddedInflux == null) {
            throw FailureHandler.createIllegalStateFailure("There is no EmbeddedInflux rule with default target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
        }

        if(this.influxDbConfiguration.getDatabaseName() == null) {
            throw FailureHandler.createIllegalStateFailure("There is no database defined.");
        }

        embeddedInflux.setDatabase(this.influxDbConfiguration.getDatabaseName());

        this.influxDbConfiguration.setInflux(embeddedInflux);
        return this.influxDbConfiguration;

    }

}
