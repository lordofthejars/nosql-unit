package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;
import com.marklogic.client.DatabaseClient;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

public final class MarkLogicConfiguration extends AbstractJsr330Configuration {

    /**
     * @see: <a href="https://docs.marklogic.com/guide/java/intro#id_69370">MarkLogic Server comes with a suitable REST API instance attached to the Documents database, listening on port 8000.</a>
     */
    public static final int DEFAULT_APP_PORT = 8000;

    /**
     * @see: <a href="https://docs.marklogic.com/guide/java/intro#id_69370">MarkLogic Server comes with a suitable REST API instance attached to the Documents database, listening on port 8000.</a>
     */
    public static final String DEFAULT_CONTENT_DATABASE = "Documents";

    public static final String DEFAULT_COLLECTION = "nosqlunit";

    public static final String DEFAULT_HOST = "localhost";

    private static final String DEFAULT_USERNAME = "admin";

    private static final String DEFAULT_PASSWORD = "admin";

    /**
     * The host where the REST server resides
     */
    private String host;

    /**
     * The port, the REST server listens on
     */
    private int port;

    /**
     * Should the secure connection to the REST server be established?
     */
    private boolean secure;

    /**
     * Whether a an LB or an other sort of gateway, for clustered setup.
     */
    private boolean useGateway;

    private String username = DEFAULT_USERNAME;

    private String password = DEFAULT_PASSWORD;

    /**
     * The database to access (default: configured database for the REST server)
     */
    private String database;

    /**
     * Collections to be managed
     */
    private Set<String> cleanCollections = new HashSet<>();

    /**
     * The optional cleanDirectory to be managed
     */
    private String cleanDirectory;

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

    public boolean isUseGateway() {
        return useGateway;
    }

    public void setUseGateway(boolean useGateway) {
        this.useGateway = useGateway;
    }

    public DatabaseClient getDatabaseClient() {
        return databaseClient;
    }

    public void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public String[] getCleanCollections() {
        return cleanCollections.toArray(new String[0]);
    }

    public void setCleanCollections(String... cleanCollections) {
        this.cleanCollections = new HashSet<>(asList(cleanCollections));
    }

    public String getCleanDirectory() {
        return cleanDirectory;
    }

    public void setCleanDirectory(String cleanDirectory) {
        this.cleanDirectory = cleanDirectory;
    }
}
