
package com.lordofthejars.nosqlunit.influxdb;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

public class InfluxDbConfigurationBuilder {

    private final InfluxDbConfiguration influxDbConfiguration;

    public static InfluxDbConfigurationBuilder influxDb() {
        return new InfluxDbConfigurationBuilder();
    }

    private InfluxDbConfigurationBuilder() {
        influxDbConfiguration = new InfluxDbConfiguration();
    }

    public InfluxDbConfigurationBuilder databaseName(final String databaseName) {
        influxDbConfiguration.setDatabaseName(databaseName);
        return this;
    }

    public InfluxDbConfiguration build() {
        final InfluxDB influx;
        final String databaseUrl = influxDbConfiguration.getUrl();
        if (influxDbConfiguration.isAuthenticateParametersSet()) {
            influx = InfluxDBFactory.connect(databaseUrl, influxDbConfiguration.getUsername(),
                    influxDbConfiguration.getPassword());
        } else {
            influx = InfluxDBFactory.connect(databaseUrl);
        }

        influx.setDatabase(influxDbConfiguration.getDatabaseName());

        this.influxDbConfiguration.setInflux(influx);
        return influxDbConfiguration;
    }

}
