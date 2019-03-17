package com.lordofthejars.nosqlunit.marklogic;

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
import java.net.ConnectException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.http.impl.client.HttpClients.custom;

public class MarkLogicLowLevelOps {

    private static final Logger LOGGER = LoggerFactory.getLogger(MarkLogicLowLevelOps.class);

    private static final int DELAY = 1;

    private static final int DELAY_FACTOR = 2;

    private static final int MAX_RETRIES = 20;

    MarkLogicLowLevelOps() {
    }

    public boolean assertThatConnectionIsPossible(String host, int port, String url, String user, String password) throws IOException, InterruptedException {
        return waitForStatus(host, port, url, user, password, 200);
    }

    public boolean assertThatConnectionIsNotPossible(String host, int port, String url, String user, String password) throws IOException, InterruptedException {
        try {
            return waitForStatus(host, port, url, user, password, 404);
        } catch (ConnectException e) {
            return true;
        }
    }

    public boolean waitForStatus(String host, int port, String url, String user, String password, int statusToWaitFor) throws IOException, InterruptedException {
        int currentRetry = MAX_RETRIES;
        boolean statusReached = false;
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(new AuthScope(host, port), new UsernamePasswordCredentials(user, password));
        HttpGet get = new HttpGet(url);
        // Create AuthCache instance
        AuthCache authCache = new BasicAuthCache();
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setAuthCache(authCache);
        int delay = DELAY;
        try (CloseableHttpClient client = custom().setDefaultCredentialsProvider(credsProvider).build()) {
            do {
                SECONDS.sleep(delay * DELAY_FACTOR);
                try (CloseableHttpResponse response = client.execute(get, localContext)) {
                    statusReached = statusToWaitFor == response.getStatusLine().getStatusCode();
                    currentRetry--;
                }
            } while (!statusReached && currentRetry > 0);
        }
        return statusReached;
    }
}
