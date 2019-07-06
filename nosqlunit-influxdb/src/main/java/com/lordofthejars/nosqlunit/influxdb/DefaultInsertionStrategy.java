
package com.lordofthejars.nosqlunit.influxdb;

import java.io.IOException;
import java.io.InputStream;

public class DefaultInsertionStrategy implements InfluxInsertionStrategy {

    @Override
    public void insert(final InfluxDbConnectionCallback connection, final InputStream dataset) throws IOException {
        InfluxDBOperation.insertData(connection.dbClient(), dataset);
    }

}
