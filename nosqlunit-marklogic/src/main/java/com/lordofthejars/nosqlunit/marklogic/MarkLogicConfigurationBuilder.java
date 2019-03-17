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

import static com.marklogic.client.DatabaseClientFactory.SSLHostnameVerifier.ANY;
import static com.marklogic.client.DatabaseClientFactory.newClient;


public class MarkLogicConfigurationBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(MarkLogicConfigurationBuilder.class);

    private final MarkLogicConfiguration marklogicConfiguration;

    private MarkLogicConfigurationBuilder() {
        marklogicConfiguration = new MarkLogicConfiguration();
    }

    public static MarkLogicConfigurationBuilder marklogic() {
        return new MarkLogicConfigurationBuilder();
    }

    private static SSLContext sslContext() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        return SSLContexts.custom().loadTrustMaterial(null, new TrustAllStrategy()).build();
    }

    private SecurityContext securityContext(String username, String password) {
        SecurityContext result = new DigestAuthContext(username, password);
        if (!marklogicConfiguration.isSecure()) {
            return result;
        }
        try {
            result.withSSLContext(sslContext(), null).withSSLHostnameVerifier(ANY);
        } catch (Exception e) {
            LOGGER.warn("couldn't setup TLS context!", e);
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
                DatabaseClient.ConnectionType.DIRECT);
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

    public MarkLogicConfigurationBuilder adminPort(int port) {
        marklogicConfiguration.setAdminPort(port);
        return this;
    }

    public MarkLogicConfigurationBuilder secure() {
        marklogicConfiguration.setSecure(true);
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

    public MarkLogicConfigurationBuilder connectionIdentifier(String identifier) {
        marklogicConfiguration.setConnectionIdentifier(identifier);
        return this;
    }
}
