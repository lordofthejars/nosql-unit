package com.lordofthejars.nosqlunit.couchdb;

import org.apache.http.conn.ssl.SSLSocketFactory;
import org.ektorp.CouchDbConnector;

import static com.lordofthejars.nosqlunit.couchdb.CouchDbConnectorFactory.couchDbConnector;

public class ManagedCouchDbConfigurationBuilder {

    private CouchDbConfiguration couchDbConfiguration;

    private ManagedCouchDbConfigurationBuilder() {
        this.couchDbConfiguration = new CouchDbConfiguration();
    }

    public static ManagedCouchDbConfigurationBuilder newManagedCouchDbConfiguration() {
        return new ManagedCouchDbConfigurationBuilder();
    }

    public ManagedCouchDbConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
        this.couchDbConfiguration.setConnectionIdentifier(connectionIdentifier);
        return this;
    }

    public ManagedCouchDbConfigurationBuilder url(String url) {
        this.couchDbConfiguration.setUrl(url);
        return this;
    }

    public ManagedCouchDbConfigurationBuilder username(String username) {
        this.couchDbConfiguration.setUsername(username);
        return this;
    }

    public ManagedCouchDbConfigurationBuilder password(String password) {
        this.couchDbConfiguration.setPassword(password);
        return this;
    }

    public ManagedCouchDbConfigurationBuilder caching(boolean caching) {
        this.couchDbConfiguration.setCaching(caching);
        return this;
    }

    public ManagedCouchDbConfigurationBuilder enableSsl(boolean enableSsl) {
        this.couchDbConfiguration.setEnableSsl(enableSsl);
        return this;
    }

    public ManagedCouchDbConfigurationBuilder relaxedSsl(boolean relaxedSsl) {
        this.couchDbConfiguration.setRelaxedSsl(relaxedSsl);
        return this;
    }

    public ManagedCouchDbConfigurationBuilder databaseName(String databaseName) {
        this.couchDbConfiguration.setDatabaseName(databaseName);
        return this;
    }

    public ManagedCouchDbConfigurationBuilder sslSocketFactory(SSLSocketFactory socketFactory) {
        this.couchDbConfiguration.setSslSocketFactory(socketFactory);
        return this;
    }

    public CouchDbConfiguration build() {

        CouchDbConnector couchDbConnector = couchDbConnector(this.couchDbConfiguration);
        this.couchDbConfiguration.setCouchDbConnector(couchDbConnector);

        return this.couchDbConfiguration;
    }
}
