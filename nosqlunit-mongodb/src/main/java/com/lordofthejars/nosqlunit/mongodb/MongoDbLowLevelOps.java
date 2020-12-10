package com.lordofthejars.nosqlunit.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MongoDbLowLevelOps {

    private static final String MEMBERS_TOKEN = "members";

    private static final int WAIT_TIME = 6;
    private static final int MAX_RETRIES = 20;

    private static final String STATE_TOKEN = "state";

    private static final Integer STARTING_UP_1 = 0;
    private static final Integer PRIMARY = 1;
    private static final Integer SECONDARY = 2;
    private static final Integer RECOVERING = 3;
    private static final Integer FATAL_ERROR = 4;
    private static final Integer STARTING_UP_2 = 5;
    private static final Integer UNKNOWN = 6;
    private static final Integer ARBITER = 7;
    private static final Integer DOWN = 8;

    MongoDbLowLevelOps() {
        super();
    }

    public void waitUntilReplicaSetBecomeStable(MongoClient mongoClient,
                                                int numberOfServersStable, String... authenticateParameters) {

        boolean isConfigurationSpread = false;

        int retries = 0;

        while (!isConfigurationSpread) {

            try {
                TimeUnit.SECONDS.sleep(WAIT_TIME);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }

            Document status = null;
            try {
                status = getStatus(mongoClient, authenticateParameters);
                isConfigurationSpread = isSystemStable(status,
                        numberOfServersStable);
            } catch (MongoException e) {
                status = new Document("MongoException", "can't find a master");
                isConfigurationSpread = false;
            }

            retries++;

            if (retries > MAX_RETRIES && !isConfigurationSpread) {
                mongoClient.close();
                throw new IllegalStateException(
                        "After "
                                + (WAIT_TIME * MAX_RETRIES)
                                + " seconds replica set scenario could not be started and configured. Last status message was: "
                                + status.toJson());
            }

        }

    }

    private Document getStatus(MongoClient mongoClient,
                               String... authenticateParameters) {
        Document status = null;
        if (authenticateParameters.length == 2) {
            status = getMongosStatus(mongoClient, authenticateParameters[0],
                    authenticateParameters[1]);
        } else {
            status = getMongosStatus(mongoClient);
        }
        return status;
    }

    private boolean isSystemStable(Document status, int numberOfServersStable) {

        int currentNumberOfStables = 0;

        List<Document> members = status.get(MEMBERS_TOKEN, List.class);

        if (members == null) {
            return false;
        }

        for (Document member : members) {

            if (member.get(STATE_TOKEN).equals(PRIMARY)
                    || member.get(STATE_TOKEN).equals(SECONDARY)
                    || member.get(STATE_TOKEN).equals(ARBITER)) {
                currentNumberOfStables++;
            }
        }

        return currentNumberOfStables == numberOfServersStable;
    }

    private Document getMongosStatus(MongoClient mongoClient, String username,
                                     String password) {
        return MongoDbCommands.replicaSetGetStatus(mongoClient);
    }

    private Document getMongosStatus(MongoClient mongoClient) {
        return MongoDbCommands.replicaSetGetStatus(mongoClient);
    }

    public boolean assertThatConnectionIsPossible(String host, int port) throws InterruptedException, UnknownHostException,
            MongoException {

        int currentRetry = 0;
        boolean connectionIsPossible = false;

        MongoClient server = null;
        try {
            do {
                TimeUnit.SECONDS.sleep(WAIT_TIME);

                server = MongoClients.create(
                        MongoClientSettings.builder()
                                .applyToClusterSettings(builder ->
                                        builder.hosts(Arrays.asList(
                                                new ServerAddress(host, port))))
                                .build());

                MongoDatabase db = server.getDatabase("admin");
                try {
                    db.runCommand(Document.parse("{ dbStats: 1, scale: 1 }"));
                    connectionIsPossible = true;
                } catch (MongoException e) {
                    currentRetry++;
                }
            } while (!connectionIsPossible && currentRetry <= MAX_RETRIES);
        } finally {
            if (server != null)
                server.close();
        }

        return connectionIsPossible;
    }

    public void shutdown(String host, int port) {
        MongoDbCommands.shutdown(host, port);
    }

}
