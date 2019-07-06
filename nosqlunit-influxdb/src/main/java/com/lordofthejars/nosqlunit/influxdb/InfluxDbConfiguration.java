
package com.lordofthejars.nosqlunit.influxdb;

import org.influxdb.InfluxDB;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public final class InfluxDbConfiguration extends AbstractJsr330Configuration {

    private static final String DEFAULT_URL = "http://localhost:8086";

    private String databaseName;

    private String username;

    private String password;

    private String url = DEFAULT_URL;

    private InfluxDB influx;

    public InfluxDbConfiguration() {
        super();
    }

    public InfluxDbConfiguration(final String url, final String databaseName) {
        super();
        this.url = url;
        this.databaseName = databaseName;
    }

    public InfluxDbConfiguration(final String databaseName, final String username, final String password) {
        super();
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(final String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(final String url) {
        this.url = url;
    }

    public InfluxDB getInflux() {
        return influx;
    }

    public void setInflux(final InfluxDB influx) {
        this.influx = influx;
    }

    public boolean isAuthenticateParametersSet() {
        return this.username != null || this.password != null;
    }

}
