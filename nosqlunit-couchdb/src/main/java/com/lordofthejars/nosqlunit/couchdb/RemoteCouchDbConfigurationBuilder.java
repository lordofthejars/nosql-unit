package com.lordofthejars.nosqlunit.couchdb;

import org.apache.http.conn.ssl.SSLSocketFactory;
import org.ektorp.CouchDbConnector;

import static com.lordofthejars.nosqlunit.couchdb.CouchDbConnectorFactory.couchDbConnector;

public class RemoteCouchDbConfigurationBuilder {

    private CouchDbConfiguration couchDbConfiguration;

    private RemoteCouchDbConfigurationBuilder() {
        this.couchDbConfiguration = new CouchDbConfiguration();
    }

    public static RemoteCouchDbConfigurationBuilder newRemoteCouchDbConfiguration() {
        return new RemoteCouchDbConfigurationBuilder();
    }

    public RemoteCouchDbConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
        this.couchDbConfiguration.setConnectionIdentifier(connectionIdentifier);
        return this;
    }

    public RemoteCouchDbConfigurationBuilder url(String url) {
        this.couchDbConfiguration.setUrl(url);
        return this;
    }

    public RemoteCouchDbConfigurationBuilder username(String username) {
        this.couchDbConfiguration.setUsername(username);
        return this;
    }

    public RemoteCouchDbConfigurationBuilder password(String password) {
        this.couchDbConfiguration.setPassword(password);
        return this;
    }

    public RemoteCouchDbConfigurationBuilder caching(boolean caching) {
        this.couchDbConfiguration.setCaching(caching);
        return this;
    }

    public RemoteCouchDbConfigurationBuilder enableSsl(boolean enableSsl) {
        this.couchDbConfiguration.setEnableSsl(enableSsl);
        return this;
    }

    public RemoteCouchDbConfigurationBuilder relaxedSsl(boolean relaxedSsl) {
        this.couchDbConfiguration.setRelaxedSsl(relaxedSsl);
        return this;
    }

    public RemoteCouchDbConfigurationBuilder databaseName(String databaseName) {
        this.couchDbConfiguration.setDatabaseName(databaseName);
        return this;
    }

    public RemoteCouchDbConfigurationBuilder sslSocketFactory(SSLSocketFactory socketFactory) {
        this.couchDbConfiguration.setSslSocketFactory(socketFactory);
        return this;
    }

    public CouchDbConfiguration build() {

        CouchDbConnector couchDbConnector = couchDbConnector(this.couchDbConfiguration);
        this.couchDbConfiguration.setCouchDbConnector(couchDbConnector);

        return this.couchDbConfiguration;
    }
}
