package com.lordofthejars.nosqlunit.marklogic;

import org.apache.http.NoHttpResponseException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.http.impl.client.HttpClients.custom;

public class MarkLogicLowLevelOps {

    private static final Logger log = LoggerFactory.getLogger(MarkLogicLowLevelOps.class);

    private static final int MAX_ATTEMPTS = 30;

    MarkLogicLowLevelOps() {
    }

    private static void waitForAWhile() {
        try {
            SECONDS.sleep(3);
        } catch (InterruptedException e) {
        }
    }

    public boolean assertThatConnectionIsPossible(String host, int port, String url, String user, String password) throws IOException {
        return waitForStatus(host, port, url, user, password, 200);
    }

    public boolean assertThatConnectionIsNotPossible(String host, int port, String url, String user, String password) throws IOException {
        return waitForStatus(host, port, url, user, password, 404);
    }

    public boolean waitForStatus(String host, int port, String url, String user, String password, final int statusToWaitFor) throws IOException {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(host, port), new UsernamePasswordCredentials(user, password));
        HttpGet get = new HttpGet(url);
        // Create AuthCache instance
        AuthCache authCache = new BasicAuthCache();
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setAuthCache(authCache);
        int currentStatus = -1;
        int attempts = MAX_ATTEMPTS;
        try (CloseableHttpClient client = custom()
                .setDefaultCredentialsProvider(credentialsProvider)
                .disableAutomaticRetries()
                .build()) {
            while (currentStatus != statusToWaitFor && attempts-- > 0) {
                waitForAWhile();
                try (CloseableHttpResponse response = client.execute(get, localContext)) {
                    currentStatus = response.getStatusLine().getStatusCode();
                } catch (NoHttpResponseException | SocketException e) {
                    //roughly reached the desired state in case we are waiting for the connection to be broken
                    if (statusToWaitFor > 300) {
                        return true;
                    }
                }
            }
        }
        return currentStatus == statusToWaitFor;
    }
}
