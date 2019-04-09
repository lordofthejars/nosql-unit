package com.lordofthejars.nosqlunit.marklogic.integration;

import com.lordofthejars.nosqlunit.marklogic.DockerTestRunner;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicLifecycleManager;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.DatabaseClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import javax.inject.Named;

import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicConfigurationBuilder.marklogic;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicLifecycleManagerBuilder.newManagedMarkLogicLifecycle;
import static com.lordofthejars.nosqlunit.marklogic.ml.MarkLogicREST.createRESTServerWithDB;
import static com.lordofthejars.nosqlunit.marklogic.ml.MarkLogicREST.deleteRESTServerWithDB;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Uses a remote MarkLogic instance for testing (which is dockerized, having container name 'marklogic').
 * Tests rules with two different MarkLogic databases.
 * To be able to run this tests with docker, please make sure the corresponding container ports
 * (TEST_PORT_1 and TEST_PORT_2) are mapped to the host's ones.
 */
@RunWith(DockerTestRunner.class)
public class WhenMultipleDatabaseClientsAreAnnotatedWithInject {

    private static final int TEST_PORT_1 = 9001;

    private static final int TEST_PORT_2 = 9002;

    private static final String TEST_CONTAINER_NAME = "marklogic";

    private static ManagedMarkLogicLifecycleManager lifecycle;

    @Rule
    public MarkLogicRule remoteMarkLogicRule1 = new MarkLogicRule(marklogic().connectionIdentifier("one").build(), this);

    @Rule
    public MarkLogicRule remoteMarkLogicRule2 = new MarkLogicRule(marklogic().connectionIdentifier("two").build(), this);

    @Named("one")
    @Inject
    private DatabaseClient databaseClient1;

    @Named("two")
    @Inject
    private DatabaseClient databaseClient2;

    @BeforeClass
    public static void createDatabases() throws Throwable {
        lifecycle = newManagedMarkLogicLifecycle().dockerContainer(TEST_CONTAINER_NAME).get();
        lifecycle.doStart();
        createRESTServerWithDB("test-one", TEST_PORT_1);
        createRESTServerWithDB("test-two", TEST_PORT_2);
    }

    @AfterClass
    public static void deleteDatabases() throws Throwable {
        deleteRESTServerWithDB("test-one");
        deleteRESTServerWithDB("test-two");
        lifecycle.doStop();
    }

    @Test
    public void database_client_used_in_rule_should_be_injected() throws Throwable {
        assertThat(databaseClient1, is(remoteMarkLogicRule1.getDatabaseOperation().connectionManager()));
        assertThat(databaseClient2, is(remoteMarkLogicRule2.getDatabaseOperation().connectionManager()));
    }
}
