package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;
import com.marklogic.client.DatabaseClient;

public final class MarkLogicConfiguration extends AbstractJsr330Configuration {

    public static final String DEFAULT_COLLECTION = "nosqlunit";

    private static final String DEFAULT_HOST = "localhost";

    private static final int DEFAULT_PORT = 8001;

    private static final String DEFAULT_USERNAME = "admin";

    private static final String DEFAULT_PASSWORD = "admin";

    /**
     * The host where the REST server resides
     */
    private String host = DEFAULT_HOST;

    /**
     * The host, the REST server listens on
     */
    private int port = DEFAULT_PORT;

    /**
     * Should the secure connection to the REST server be established?
     */
    private boolean secure;

    private String username = DEFAULT_USERNAME;

    private String password = DEFAULT_PASSWORD;

    /**
     * The database to access (default: configured database for the REST server)
     */
    private String database;

    private DatabaseClient databaseClient;

    public MarkLogicConfiguration() {
    }

    public MarkLogicConfiguration(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public MarkLogicConfiguration(String host, int port, String username, String password) {
        this(host, port);
        this.username = username;
        this.password = password;
    }

    public MarkLogicConfiguration(String host, int port, String username, String password, boolean secure) {
        this(host, port, username, password);
        this.secure = secure;
    }

    public MarkLogicConfiguration(String host, int port, String username, String password, boolean secure, String database) {
        this(host, port, username, password, secure);
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public DatabaseClient getDatabaseClient() {
        return databaseClient;
    }

    public void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }
}
