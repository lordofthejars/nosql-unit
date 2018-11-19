
package com.lordofthejars.nosqlunit.dynamodb;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class InMemoryDynamoDbLifecycleManager extends AbstractLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryDynamoDb.class);

    private static final String LOCALHOST = "localhost";

    private static final int PORT = 8000;

    private static final String ENDPOINT = String.format("http://%s:%s", LOCALHOST, PORT);

    public static final String INMEMORY_MONGO_TARGET_PATH = "target" + File.separatorChar + "dynamo-data"
            + File.separatorChar + "impermanent-db";

    private String targetPath = INMEMORY_MONGO_TARGET_PATH;

    private DynamoDBProxyServer server;

    @Override
    public String getHost() {
        return LOCALHOST + targetPath;
    }

    @Override
    public int getPort() {
        return PORT;
    }

    @Override
    public void doStart() throws Throwable {
        LOGGER.info("Starting EmbeddedInMemory DynamoDb instance.");
        EmbeddedDynamoInstancesFactory.getInstance().addEmbeddedInstance(dynamodbLocal(targetPath), targetPath);
        LOGGER.info("Started EmbeddedInMemory DynamoDb instance.");

    }

    @SuppressWarnings("deprecation")
    private AmazonDynamoDB dynamodbLocal(final String targetPath) throws Exception {
        System.setProperty("sqlite4java.library.path", "native-libs");
        server = ServerRunner.createServerFromCommandLineArgs(
                new String[] { "-inMemory", "-port", String.valueOf(PORT) });
        server.start();

        final AmazonDynamoDB client = new AmazonDynamoDBClient();
        client.setEndpoint(ENDPOINT);
        return client;
    }

    @Override
    public void doStop() {
        LOGGER.info("Stopping EmbeddedInMemory DynamoDb instance.");
        EmbeddedDynamoInstancesFactory.getInstance().removeEmbeddedInstance(targetPath);
        try {
            server.stop();
        } catch (final Exception e) {
            LOGGER.error("Exception in stopping EmbeddedInMemory DynamoDb instance", e);
        }
        LOGGER.info("Stopped EmbeddedInMemory DynamoDb instance.");
    }

    public void setTargetPath(final String targetPath) {
        this.targetPath = targetPath;
    }

    public String getTargetPath() {
        return targetPath;
    }

}
