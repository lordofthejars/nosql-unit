package com.lordofthejars.nosqlunit.marklogic.integration;

import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicOperation;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicConfigurationBuilder.marklogic;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class WhenManagedMarkLogicOperationsAreRequired {

    @ClassRule
    public static final ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();

    private static final String DATA = "{ " +
            "    \"/data/alpha/1.json\": { \"data\": {" +
            "        \"id\": 1, \"code\": \"alpha JSON dataset\"" +
            "        }," +
            "        \"collections\" : [ \"alpha\"]" +
            "    }, " +
            "    \"/data/bravo/2.json\": { \"data\": {" +
            "        \"id\": 2, \"code\": \"bravo JSON dataset\"" +
            "        },\n" +
            "        \"collections\" : [ \"bravo\"]" +
            "    }" +
            "}";

    @After
    public void tearDown() {
    }

    @Test
    public void data_should_be_inserted_into_marklogic() {
        MarkLogicOperation markLogicOperation = new MarkLogicOperation(marklogic().build());
        markLogicOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        DatabaseClient client = markLogicOperation.connectionManager();
        QueryManager qm = client.newQueryManager();
        MatchDocumentSummary summary = qm.findOne(qm.newStringDefinition().withCriteria("JSON"));
        assertThat(summary, is(notNullValue()));
    }

    @Test
    public void all_data_should_be_removed_from_marklogic() {
        MarkLogicOperation markLogicOperation = new MarkLogicOperation(marklogic().build());
        markLogicOperation.insert(new ByteArrayInputStream(DATA.getBytes()));
        markLogicOperation.deleteAll();

        DatabaseClient client = markLogicOperation.connectionManager();
        QueryManager qm = client.newQueryManager();
        MatchDocumentSummary summary = qm.findOne(qm.newStringDefinition().withCriteria("JSON"));
        assertThat(summary, is(nullValue()));
    }

    @Test
    public void directory_data_should_be_removed_from_marklogic() {
        MarkLogicOperation markLogicOperation = new MarkLogicOperation(marklogic().cleanDirectory("/data/bravo/").build());
        markLogicOperation.insert(new ByteArrayInputStream(DATA.getBytes()));
        markLogicOperation.deleteAll();

        DatabaseClient client = markLogicOperation.connectionManager();
        QueryManager qm = client.newQueryManager();
        MatchDocumentSummary summary = qm.findOne(qm.newStringDefinition().withCriteria("alpha JSON"));
        assertThat(summary, is(notNullValue()));

        summary = qm.findOne(qm.newStringDefinition().withCriteria("bravo JSON"));
        assertThat(summary, is(nullValue()));
    }

    @Test
    public void collection_data_should_be_removed_from_marklogic() {
        MarkLogicOperation markLogicOperation = new MarkLogicOperation(marklogic().cleanCollections("alpha").build());
        markLogicOperation.insert(new ByteArrayInputStream(DATA.getBytes()));
        markLogicOperation.deleteAll();

        DatabaseClient client = markLogicOperation.connectionManager();
        QueryManager qm = client.newQueryManager();
        MatchDocumentSummary summary = qm.findOne(qm.newStringDefinition().withCriteria("bravo JSON"));
        assertThat(summary, is(notNullValue()));

        summary = qm.findOne(qm.newStringDefinition().withCriteria("alpha JSON"));
        assertThat(summary, is(nullValue()));
    }

    @Test
    public void expected_and_current_data_coparison_should_succeed() {
        MarkLogicOperation markLogicOperation = new MarkLogicOperation(marklogic().build());
        markLogicOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        boolean result = markLogicOperation.databaseIs(new ByteArrayInputStream(DATA.getBytes()));
        assertThat(result, is(true));
    }
}
