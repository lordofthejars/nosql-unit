package com.lordofthejars.nosqlunit.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext;
import com.marklogic.client.DatabaseClientFactory.SecurityContext;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import static com.marklogic.client.DatabaseClient.ConnectionType.DIRECT;
import static com.marklogic.client.DatabaseClient.ConnectionType.GATEWAY;
import static com.marklogic.client.DatabaseClientFactory.SSLHostnameVerifier.ANY;
import static com.marklogic.client.DatabaseClientFactory.newClient;


public abstract class MarkLogicConfigurationBuilder {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final MarkLogicConfiguration marklogicConfiguration;

    protected MarkLogicConfigurationBuilder() {
        marklogicConfiguration = new MarkLogicConfiguration();
    }

    protected static SSLContext sslContext() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        return SSLContexts.custom().loadTrustMaterial(null, new TrustAllStrategy()).build();
    }

    protected SecurityContext securityContext(String username, String password) {
        SecurityContext result = new DigestAuthContext(username, password);
        if (!marklogicConfiguration.isSecure()) {
            return result;
        }
        try {
            result.withSSLContext(sslContext(), null).withSSLHostnameVerifier(ANY);
        } catch (Exception e) {
            log.warn("couldn't setup TLS context!", e);
        }
        return result;
    }

    public MarkLogicConfiguration build() {
        DatabaseClient databaseClient = newClient(
                marklogicConfiguration.getHost(),
                marklogicConfiguration.getPort(),
                marklogicConfiguration.getDatabase(),
                securityContext(
                        marklogicConfiguration.getUsername(),
                        marklogicConfiguration.getPassword()
                ),
                marklogicConfiguration.isUseGateway() ? GATEWAY : DIRECT);
        marklogicConfiguration.setDatabaseClient(databaseClient);
        return marklogicConfiguration;
    }

    public MarkLogicConfigurationBuilder host(String host) {
        marklogicConfiguration.setHost(host);
        return this;
    }

    public MarkLogicConfigurationBuilder port(int port) {
        marklogicConfiguration.setPort(port);
        return this;
    }

    public MarkLogicConfigurationBuilder secure() {
        marklogicConfiguration.setSecure(true);
        return this;
    }

    public MarkLogicConfigurationBuilder useGateway() {
        marklogicConfiguration.setUseGateway(true);
        return this;
    }

    public MarkLogicConfigurationBuilder username(String username) {
        marklogicConfiguration.setUsername(username);
        return this;
    }

    public MarkLogicConfigurationBuilder password(String password) {
        marklogicConfiguration.setPassword(password);
        return this;
    }

    public MarkLogicConfigurationBuilder database(String database) {
        marklogicConfiguration.setDatabase(database);
        return this;
    }

    /**
     * Defines the directory to manage during clean-up, default: all
     *
     * @param directory to be cleaned
     * @return this builder
     */
    public MarkLogicConfigurationBuilder cleanDirectory(String directory) {
        marklogicConfiguration.setCleanDirectory(directory);
        return this;
    }

    /**
     * Defines collections to manage during clean-up, default: all
     *
     * @param collections to be cleaned
     * @return this builder
     */
    public MarkLogicConfigurationBuilder cleanCollections(String... collections) {
        marklogicConfiguration.setCleanCollections(collections);
        return this;
    }

    public MarkLogicConfigurationBuilder connectionIdentifier(String identifier) {
        marklogicConfiguration.setConnectionIdentifier(identifier);
        return this;
    }
}
