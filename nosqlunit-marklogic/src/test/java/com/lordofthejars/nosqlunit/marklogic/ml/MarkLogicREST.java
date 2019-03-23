package com.lordofthejars.nosqlunit.marklogic.ml;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicLowLevelOpsFactory.getSingletonInstance;
import static com.lordofthejars.nosqlunit.marklogic.ml.DefaultMarkLogic.PROPERTIES;
import static java.lang.String.format;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpStatus.SC_UNAUTHORIZED;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.apache.http.impl.client.HttpClients.custom;

/**
 * Utility class, can be used to manege MarkLogic runtime artifacts (RESTful servers, databases, forests etc.)
 * via REST-management API.
 */
public abstract class MarkLogicREST {

    private static final Logger log = LoggerFactory.getLogger(MarkLogicREST.class);

    private static final String ALIVE_URL = "http://%s:%d/admin/v1/timestamp";

    private MarkLogicREST() {
    }

    /*
     * Creates an application server having a default content and module databases.
     * Uses default settings for a MarkLogic database.
     */
    public static void createRESTServerWithDB(String serverName, int port) throws IOException {
        createRESTServerWithDB(PROPERTIES.adminHost, PROPERTIES.mgmtPort, PROPERTIES.adminUser, PROPERTIES.adminPassword, serverName, port);
    }

    /*
     * Creates an application server having a default content and module databases
     */
    public static void createRESTServerWithDB(String adminHost, int mgmtPort, String user, String password, String serverName, int port) throws IOException {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(adminHost, mgmtPort), new UsernamePasswordCredentials(user, password));
        // Create AuthCache instance
        AuthCache authCache = new BasicAuthCache();
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setAuthCache(authCache);
        HttpPost post = new HttpPost("http://" + adminHost + ":" + mgmtPort + "/v1/rest-apis?format=json");
        String command = "{  \"rest-api\": {" +
                "    \"name\": \"" + serverName + "\"," +
                "    \"database\": \"" + serverName + "-content\"," +
                "    \"port\": \"" + port + "\"," +
                "    \"forests-per-host\": \"1\"" +
                "  }" +
                "}";
        post.addHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType());
        post.setEntity(new StringEntity(command));
        String response = authNExec(adminHost, mgmtPort, user, password, post);
        log.info("Created REST app server: {}, listening at: {}, response: {}", serverName, port, response);
    }

    /**
     * Deletes an application server along with attached content and module databases.
     * Uses default settings for a MarkLogic database.
     */
    public static void deleteRESTServerWithDB(String serverName) throws IOException {
        deleteRESTServerWithDB(PROPERTIES.adminHost, PROPERTIES.mgmtPort, PROPERTIES.adminUser, PROPERTIES.adminPassword, serverName);
    }

    /*
     * Deletes an application server along with attached content and module databases
     */
    public static void deleteRESTServerWithDB(String adminHost, int mgmtPort, String user, String password, String serverName) throws IOException {
        HttpDelete delete = new HttpDelete("http://" + adminHost + ":" + mgmtPort + "/v1/rest-apis/" + serverName + "?include=content&include=modules");
        String response = authNExec(adminHost, mgmtPort, user, password, delete);
        log.info("Deleted REST app server: {}, response: {}", serverName, response);
    }

    private static String authNExec(String host, int port, String user, String password, HttpRequestBase request) throws IOException {
        String result = null;
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(host, port), new UsernamePasswordCredentials(user, password));
        // Create AuthCache instance
        AuthCache authCache = new BasicAuthCache();
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setAuthCache(authCache);
        int responseCode = SC_UNAUTHORIZED;
        try (CloseableHttpClient client = custom().setDefaultCredentialsProvider(credentialsProvider).build()) {
            //we need 2 request for the digest authentication
            int attempts = 2;
            while (attempts-- > 0 && responseCode == SC_UNAUTHORIZED) {
                try (CloseableHttpResponse response = client.execute(request, localContext)) {
                    StatusLine statusLine = response.getStatusLine();
                    responseCode = statusLine.getStatusCode();
                    HttpEntity responseEntity = response.getEntity();
                    log.info("status: {}", statusLine);
                    //read the response fully before the stream is closed
                    if (responseEntity != null) {
                        result = EntityUtils.toString(responseEntity);
                    }
                }
            }
        }
        return result;
    }

    /**
     * MarkLogic does restart after it's sent 'accepted' response
     */
    public static void waitForServerAvailable() {
        try {
            getSingletonInstance().assertThatConnectionIsPossible(
                    PROPERTIES.adminHost,
                    PROPERTIES.adminPort,
                    format(ALIVE_URL,
                            PROPERTIES.adminHost,
                            PROPERTIES.adminPort
                    ),
                    PROPERTIES.adminUser, PROPERTIES.adminPassword
            );
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }
}
