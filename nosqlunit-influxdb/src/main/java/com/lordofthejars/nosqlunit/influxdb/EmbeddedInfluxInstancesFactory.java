package com.lordofthejars.nosqlunit.influxdb;

import org.influxdb.InfluxDB;

import com.lordofthejars.nosqlunit.util.EmbeddedInstances;

public class EmbeddedInfluxInstancesFactory {

    private static EmbeddedInstances<InfluxDB> embeddedInstances;

    private EmbeddedInfluxInstancesFactory() {
        super();
    }

    public synchronized static EmbeddedInstances<InfluxDB> getInstance() {
        if(embeddedInstances == null) {
            embeddedInstances = new EmbeddedInstances<>();
        }

        return embeddedInstances;
    }

}
