package com.lordofthejars.nosqlunit.mongodb.integration;

import com.lordofthejars.nosqlunit.mongodb.MongoDbCommands;
import com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetManagedMongoDb;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManagerBuilder.newManagedMongoDbLifecycle;
import static com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetBuilder.replicaSet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WhenReplicaSetIsRequired {

    static {
        System.setProperty("MONGO_HOME", "/opt/mongo");
    }

    @ClassRule
    public static ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs-test")
            .eligible(
                    newManagedMongoDbLifecycle().port(27017).dbRelativePath("rs-0").logRelativePath("log-0").get()
            )
            .eligible(
                    newManagedMongoDbLifecycle().port(27018).dbRelativePath("rs-1").logRelativePath("log-1").get()
            )
            .eligible(
                    newManagedMongoDbLifecycle().port(27019).dbRelativePath("rs-2").logRelativePath("log-2").get()
            )
            .get();

    @AfterClass
    public static void tearDown() {
        System.clearProperty("MONGO_HOME");
    }

    @Test
    public void three_member_set_scenario_should_be_started() throws UnknownHostException, InterruptedException {

        MongoClient mongoClient = MongoClients.create(MongoClientSettings
                .builder()
                .applyToClusterSettings(b -> b.hosts(Arrays.asList(new ServerAddress("localhost", 27017))))
                .build());

        Document replicaSetGetStatus = MongoDbCommands.replicaSetGetStatus(mongoClient);
        assertThat(countPrimary(replicaSetGetStatus), is(1));
        assertThat(countSecondaries(replicaSetGetStatus), is(2));

        mongoClient.close();

    }

    @Test
    public void server_should_be_able_to_stopped_programmatically() throws UnknownHostException {

        replicaSetManagedMongoDb.shutdownServer(27017);
        replicaSetManagedMongoDb.waitUntilReplicaSetBecomesStable();

        MongoClient mongoClient = MongoClients.create(MongoClientSettings
                .builder()
                .applyToClusterSettings(b -> b.hosts(Arrays.asList(new ServerAddress("localhost", 27017))))
                .build());

        Document replicaSetGetStatus = MongoDbCommands.replicaSetGetStatus(mongoClient);

        assertThat(countPrimary(replicaSetGetStatus), is(1));
        assertThat(countSecondaries(replicaSetGetStatus), is(1));

        mongoClient.close();

    }

    private int countSecondaries(Document configuration) {
        return countStates(configuration, "SECONDARY");
    }

    private int countPrimary(Document configuration) {
        return countStates(configuration, "PRIMARY");
    }

    private int countStates(Document configuration, String wantedState) {
        int number = 0;

        List<Document> basicDBList = configuration.get("members", List.class);

        for (Document object : basicDBList) {

            String state = object.getString("stateStr");

            if (state.equalsIgnoreCase(wantedState)) {
                number++;
            }

        }
        return number;
    }

}
