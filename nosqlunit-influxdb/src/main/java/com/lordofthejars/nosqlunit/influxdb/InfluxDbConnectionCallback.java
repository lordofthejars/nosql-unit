
package com.lordofthejars.nosqlunit.influxdb;

import org.influxdb.InfluxDB;

public interface InfluxDbConnectionCallback {

    InfluxDB dbClient();
}
