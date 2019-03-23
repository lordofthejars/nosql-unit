package com.lordofthejars.nosqlunit.marklogic.ml;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
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

/**
 * Utility class, can be used to query MarkLogic databases for content via REST API.
 */
public abstract class DefaultMarkLogic {

    public static final DefaultMarkLogicProperties PROPERTIES = new DefaultMarkLogicProperties();

    private static final Logger log = LoggerFactory.getLogger(DefaultMarkLogic.class);

    private DefaultMarkLogic() {
    }

    public static DatabaseClient newClient(String host, int port) {
        return newClient(host, port, PROPERTIES.contentDatabase, PROPERTIES.adminUser, PROPERTIES.adminPassword, PROPERTIES.useTls);
    }

    public static DatabaseClient newClient(String host, int port, String database, String user, String password, boolean secure) {
        return DatabaseClientFactory.newClient(
                host,
                port,
                database,
                securityContext(
                        user,
                        password,
                        secure
                ),
                DatabaseClient.ConnectionType.DIRECT);
    }

    public static void close(DatabaseClient client) {
        if (client != null) {
            client.release();
        }
    }

    private static SecurityContext securityContext(String username, String password, boolean secure) {
        SecurityContext result = new DatabaseClientFactory.DigestAuthContext(username, password);
        if (!secure) {
            return result;
        }
        try {
            result.withSSLContext(sslContext(), null).withSSLHostnameVerifier(ANY);
        } catch (Exception e) {
            LoggerFactory.getLogger(DefaultMarkLogic.class).warn("couldn't setup TLS context!", e);
        }
        return result;
    }

    private static SSLContext sslContext() throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        return SSLContexts.custom().loadTrustMaterial(null, new TrustAllStrategy()).build();
    }

    public static class DefaultMarkLogicProperties {

        public final String adminHost = "localhost";

        /**
         * The application port available in the default installation.
         */
        public final int appPort = 8000;

        public final int adminPort = 8001;

        public final int mgmtPort = 8002;

        public final String adminUser = "admin";

        public final String adminPassword = "admin";

        /**
         * The application database available in the default installation.
         */
        public final String contentDatabase = "Documents";

        private final boolean useTls = false;
    }
}
