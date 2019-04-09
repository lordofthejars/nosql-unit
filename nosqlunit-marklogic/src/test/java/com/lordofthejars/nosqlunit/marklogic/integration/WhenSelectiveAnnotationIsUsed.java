package com.lordofthejars.nosqlunit.marklogic.integration;

import com.lordofthejars.nosqlunit.annotation.Selective;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.query.ExtractedItem;
import com.marklogic.client.query.ExtractedResult;
import com.marklogic.client.query.MatchDocumentSummary;
import org.junit.*;

import java.io.IOException;
import java.util.Optional;

import static ch.lambdaj.function.matcher.OrMatcher.or;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicConfigurationBuilder.marklogic;
import static com.lordofthejars.nosqlunit.marklogic.ml.DefaultMarkLogic.*;
import static com.lordofthejars.nosqlunit.marklogic.ml.MarkLogicQuery.*;
import static com.lordofthejars.nosqlunit.marklogic.ml.MarkLogicREST.createRESTServerWithDB;
import static com.lordofthejars.nosqlunit.marklogic.ml.MarkLogicREST.deleteRESTServerWithDB;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

/**
 * Tests rules with two different MarkLogic databases.
 * To be able to run this tests with docker, please make sure the corresponding ports
 * (TEST_PORT_1 and TEST_PORT_2) are mapped to the host's ones.
 */
public class WhenSelectiveAnnotationIsUsed {

    private static final int TEST_PORT_1 = 9001;

    private static final int TEST_PORT_2 = 9002;

    @ClassRule
    public static ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();

    @Rule
    public MarkLogicRule remoteMarkLogicRule1 = new MarkLogicRule(marklogic()
            .connectionIdentifier("one").port(TEST_PORT_1).build(), this);

    @Rule
    public MarkLogicRule remoteMarkLogicRule2 = new MarkLogicRule(marklogic()
            .connectionIdentifier("two").port(TEST_PORT_2).build(), this);

    @BeforeClass
    public static void createDatabases() throws IOException {
        createRESTServerWithDB("test-one", TEST_PORT_1);
        createRESTServerWithDB("test-two", TEST_PORT_2);
    }

    @AfterClass
    public static void deleteDatabases() throws IOException {
        deleteRESTServerWithDB("test-one");
        deleteRESTServerWithDB("test-two");
    }

    @Test
    @UsingDataSet(withSelectiveLocations = {
            @Selective(identifier = "one", locations = "test-one.xml"),
            @Selective(identifier = "two", locations = "test-two.xml")},
            loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    public void data_should_be_inserted_into_configured_backend() {
        DatabaseClient client1 = null;
        DatabaseClient client2 = null;
        try {
            client1 = newClient(PROPERTIES.adminHost, TEST_PORT_1);
            client2 = newClient(PROPERTIES.adminHost, TEST_PORT_2);

            Optional<ExtractedResult> fromOne = findOneByTerm(client1, "Jane");
            assertTrue(fromOne.isPresent());
            assertEquals(1, fromOne.get().size());
            ExtractedItem item = fromOne.get().next();
            assertNotNull(item);
            assertThat(item.getAs(String.class), containsString("Jane"));

            //the second data set goes into database two
            fromOne = findOneByTerm(client1, "John");
            assertFalse(fromOne.isPresent());

            Optional<SearchHandle> fromTwo = findByTerm(client2, DEFAULT_OPTIONS.pageLength, "Doe");
            assertTrue(fromTwo.isPresent());
            assertNotNull(fromTwo.get().getMatchResults());
            for (MatchDocumentSummary summary : fromTwo.get().getMatchResults()) {
                assertNotNull(summary);
                assertNotNull(summary.getExtracted());
                ExtractedResult extractedResult = summary.getExtracted();
                while (extractedResult.hasNext()) {
                    ExtractedItem i = extractedResult.next();
                    assertNotNull(i);
                    assertThat(i.getAs(String.class), or(containsString("Jane"), containsString("John")));
                }
            }
        } finally {
            close(client1);
            close(client2);
        }
    }
}
