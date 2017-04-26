package com.lordofthejars.nosqlunit.couchdb.integration;

import com.lordofthejars.nosqlunit.couchdb.CouchDbOperation;
import com.lordofthejars.nosqlunit.couchdb.ManagedCouchDb;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DbInfo;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.ClassRule;
import org.junit.Test;

import static com.lordofthejars.nosqlunit.couchdb.ManagedCouchDb.ManagedCouchDbRuleBuilder.newManagedCouchDbRule;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class WhenManagedCouchDbOperationsAreRequired {

    private static final String DATABASE_NAME = "test";

    static {
        System.setProperty("COUCHDB_HOME", "/usr/local");
    }

    private static final String COUCHDB_DATASET = "{\n" +
        "   \"data\":[\n" +
        "      {\n" +
        "         \"_id\":\"1\",\n" +
        "         \"name\":\"alex\",\n" +
        "         \"age\":\"32\"\n" +
        "      },\n" +
        "      {\n" +
        "         \"name\":\"soto\",\n" +
        "         \"age\":\"32\"\n" +
        "      }\n" +
        "   ]\n" +
        "}";

    @ClassRule
    public static ManagedCouchDb managedCouchDb = newManagedCouchDbRule().build();

    @Test
    public void insert_operation_should_add_all_dataset_to_couchDb() throws IOException {

        CouchDbOperation couchDbOperation = couchDbOperation();
        couchDbOperation.insert(new ByteArrayInputStream(COUCHDB_DATASET.getBytes()));

        CouchDbConnector connectionManager = couchDbOperation.connectionManager();

        Map<String, Object> insertedElement = connectionManager.get(Map.class, "1");
        assertThat(insertedElement, is(MapMatcher.mappedBy(expectedElementWithId())));
    }

    @Test
    public void delete_operation_should_delete_all_dataset_to_couchdb() {

        CouchDbOperation couchDbOperation = couchDbOperation();
        CouchDbConnector connectionManager = couchDbOperation.connectionManager();

        connectionManager.create(expectedElementWithId());

        couchDbOperation.deleteAll();
        DbInfo dbInfo = connectionManager.getDbInfo();
        assertThat(dbInfo.getDocCount(), is(0L));
    }

    @Test
    public void database_is_operation_should_compare_database() {

        CouchDbOperation couchDbOperation = couchDbOperation();
        couchDbOperation.insert(new ByteArrayInputStream(COUCHDB_DATASET.getBytes()));
        boolean result = couchDbOperation.databaseIs(new ByteArrayInputStream(COUCHDB_DATASET.getBytes()));
        couchDbOperation.deleteAll();
        assertThat(result, is(true));
    }

    private CouchDbOperation couchDbOperation() {
        CouchDbConnector db = couchDbConnector();
        return new CouchDbOperation(db);
    }

    private Map<String, Object> expectedElementWithId() {
        Map<String, Object> expectedMap = new HashMap<String, Object>();
        expectedMap.put("name", "alex");
        expectedMap.put("age", "32");

        return expectedMap;
    }

    private CouchDbConnector couchDbConnector() {
        HttpClient httpClient = new StdHttpClient.Builder().build();
        CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
        // if the second parameter is true, the database will be created if it doesn't exists
        CouchDbConnector db = dbInstance.createConnector(DATABASE_NAME, true);
        return db;
    }

    private static class MapMatcher extends TypeSafeMatcher<Map<String, Object>> {

        private Map<String, Object> expectedMap;

        public MapMatcher(Map<String, Object> expectedMap) {
            super();
            this.expectedMap = expectedMap;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(" different attributes ");
        }

        @Override
        protected boolean matchesSafely(Map<String, Object> map) {
            return map.get("name").equals(expectedMap.get("name")) && map.get("age").equals(expectedMap.get("age"));
        }

        @Factory
        public static <T> Matcher<Map<String, Object>> mappedBy(Map<String, Object> map) {
            return new MapMatcher(map);
        }
    }
}
