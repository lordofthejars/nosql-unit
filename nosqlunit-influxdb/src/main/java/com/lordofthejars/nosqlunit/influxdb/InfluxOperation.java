
package com.lordofthejars.nosqlunit.influxdb;

import java.io.InputStream;
import java.util.List;

import org.influxdb.InfluxDB;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public final class InfluxOperation extends AbstractCustomizableDatabaseOperation<InfluxDbConnectionCallback, InfluxDB> {

    private final InfluxDB influx;

    protected InfluxOperation(final InfluxDB influx) {
        this.influx = influx;
        this.setInsertionStrategy(new DefaultInsertionStrategy());
        this.setComparisonStrategy(new DefaultComparisonStrategy());
    }

    public InfluxOperation(final InfluxDbConfiguration influxDbConfiguration) {
        this.influx = influxDbConfiguration.getInflux();
        this.setInsertionStrategy(new DefaultInsertionStrategy());
        this.setComparisonStrategy(new DefaultComparisonStrategy());
    }

    @Override
    public void insert(final InputStream contentStream) {
        insertData(contentStream);
    }

    private void insertData(final InputStream contentStream) {
        try {

            executeInsertion(() -> influx, contentStream);

        } catch (final Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading data set file.", e);
        }
    }

    @Override
    public void deleteAll() {
        deleteAllElements(influx);
    }

    private void deleteAllElements(final InfluxDB influxDb) {
        final List<String> measurementNames = InfluxDBOperation.getAllMeasurements(influxDb);
        InfluxDBOperation.truncateMeasurements(influxDb, measurementNames);
    }

    @Override
    public boolean databaseIs(final InputStream contentStream) {

        return compareData(contentStream);

    }

    private boolean compareData(final InputStream contentStream) throws NoSqlAssertionError {
        try {
            executeComparison(() -> influx, contentStream);
            return true;
        } catch (final NoSqlAssertionError e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading expected data set file.", e);
        }
    }

    @Override
    public InfluxDB connectionManager() {
        return influx;
    }

}
