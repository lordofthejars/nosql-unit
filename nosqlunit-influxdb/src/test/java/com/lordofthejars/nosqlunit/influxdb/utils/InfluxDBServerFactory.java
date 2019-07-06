
package com.lordofthejars.nosqlunit.influxdb.utils;

import java.io.IOException;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import io.apisense.embed.influx.InfluxServer;
import io.apisense.embed.influx.configuration.InfluxConfigurationWriter;

public class InfluxDBServerFactory {

    public InfluxServer server() throws IOException {
        final InfluxConfigurationWriter influxConfig = new InfluxConfigurationWriter.Builder() //
                .setHttp(8086) //
                .build();
        return new InfluxServer.Builder().setInfluxConfiguration(influxConfig).build();
    }

    public InfluxDB client() {
        return InfluxDBFactory.connect("http://localhost:8086");
    }
}
