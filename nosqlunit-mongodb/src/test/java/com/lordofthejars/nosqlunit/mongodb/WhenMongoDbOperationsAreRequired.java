package com.lordofthejars.nosqlunit.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WhenMongoDbOperationsAreRequired {

    private static final String DATA = ""
            + "{"
            + "\"collection1\": "
            + "	["
            + "		{\"id\":1,\"code\":\"JSON dataset\",},"
            + "		{\"id\":2,\"code\":\"Another row\",}"
            + "	],"
            + "\"collection2\": "
            + "	["
            + "		{\"id\":3,\"code\":\"JSON dataset 2\",},"
            + "		{\"id\":4,\"code\":\"Another row 2\",}"
            + "	]"
            + "}";

    private static final String DATA_SHARD = ""
            + "{"
            + "\"collection1\": {"
            + "  \"shard-key-pattern\":[\"id\",\"code\"],"
            + "	\"data\":"
            + "				["
            + "					{\"id\":1,\"code\":\"JSON dataset\",},"
            + "					{\"id\":2,\"code\":\"Another row\",}"
            + "				]"
            + "   }"
            + "}";

    private static final String DATA_INDEX = ""
            + "{"
            + "\"collection1\": {"
            + "	\"indexes\": [\n"
            + "					{\n"
            + "						\"index\": {\"field\": 1}\n"
            + "					}\n"
            + "				 ],"
            + "	\"data\":"
            + "				["
            + "					{\"id\":1,\"code\":\"JSON dataset\",},"
            + "					{\"id\":2,\"code\":\"Another row\",}"
            + "				]"
            + "   }"
            + "}";

    private static final String DATA_INDEX_OPTIONS = ""
            + "{"
            + "\"collection1\": {"
            + "	\"indexes\": [\n"
            + "					{\n"
            + "						\"index\": {\"field\": 1},\n"
            + "						\"options\": {\"unique\":true}  "
            + "					}\n"
            + "				 ],"
            + "	\"data\":"
            + "				["
            + "					{\"id\":1,\"code\":\"JSON dataset\",},"
            + "					{\"id\":2,\"code\":\"Another row\",}"
            + "				]"
            + "   }"
            + "}";

    private static final String[] EXPECTED_COLLECTION_1 = new String[] {"{ \"id\" : 1, \"code\" : \"JSON dataset\" }", "{ \"id\" : 2, \"code\" : \"Another row\" }"};

    private static final String[] EXPECTED_COLLECTION_2 = new String[] {"{ \"id\" : 3, \"code\" : \"JSON dataset 2\" }", "{ \"id\" : 4, \"code\" : \"Another row 2\" }"};

    @Mock
    private MongoClient mongo;

    @Mock
    private MongoDatabase db;

    @Mock
    private MongoDatabase dbAdmin;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(mongo.getDatabase("admin")).thenReturn(dbAdmin);
        when(mongo.getDatabase("test")).thenReturn(db);
    }

    @Test
    public void insert_operation_with_indexes_should_add_data_and_indexes_into_collections() throws UnsupportedEncodingException {
        MongoCollection collection1 = mock(MongoCollection.class);

        when(db.getName()).thenReturn("test");
        when(db.getCollection("collection1")).thenReturn(collection1);

        MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost", "test"));
        mongoOperation.insert(new ByteArrayInputStream(DATA_INDEX.getBytes("UTF-8")));

        verifyInsertedData(EXPECTED_COLLECTION_1, collection1);

        Document expectedIndex = new Document("field", 1);

        verifyIndexCreationCommand(expectedIndex, collection1);
    }

    @Test
    public void insert_operation_with_options_in_indexes_should_add_data_and_indexes_into_collections() throws UnsupportedEncodingException {
        MongoCollection collection1 = mock(MongoCollection.class);

        when(db.getName()).thenReturn("test");
        when(db.getCollection("collection1")).thenReturn(collection1);

        MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost", "test"));
        mongoOperation.insert(new ByteArrayInputStream(DATA_INDEX_OPTIONS.getBytes("UTF-8")));

        verifyInsertedData(EXPECTED_COLLECTION_1, collection1);

        Document expectedIndex = new Document("field", 1);
        IndexOptions indexOptions = new IndexOptions();
        indexOptions.unique(true);

        verifyIndexCreationCommandAndOptions(expectedIndex, indexOptions, collection1);
    }

    @Test
    public void insert_opertation_with_shards_should_add_data_into_collections() throws UnsupportedEncodingException {

        MongoCollection collection1 = mock(MongoCollection.class);
        MongoCollection collection2 = mock(MongoCollection.class);

        when(db.getName()).thenReturn("test");
        when(db.getCollection("collection1")).thenReturn(collection1);
        when(db.getCollection("collection2")).thenReturn(collection2);

        MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost", "test"));
        mongoOperation.insert(new ByteArrayInputStream(DATA_SHARD.getBytes("UTF-8")));

        verifyInsertedData(EXPECTED_COLLECTION_1, collection1);
        verifyEnableShardingCommand("{ \"shardcollection\" : \"test.collection1\" , \"key\" : { \"id\" : 1 , \"code\" : 1}}", dbAdmin);
    }

    @Test
    public void insert_opertation_should_add_data_into_collections() throws UnsupportedEncodingException {

        MongoCollection collection1 = mock(MongoCollection.class);
        MongoCollection collection2 = mock(MongoCollection.class);

        when(db.getCollection("collection1")).thenReturn(collection1);
        when(db.getCollection("collection2")).thenReturn(collection2);

        MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost", "test"));
        mongoOperation.insert(new ByteArrayInputStream(DATA.getBytes("UTF-8")));

        verifyInsertedData(EXPECTED_COLLECTION_1, collection1);
        verifyInsertedData(EXPECTED_COLLECTION_2, collection2);
    }


    /*@Test
     public void same_data_on_compare_operation_should_assert_expected_data_with_inserted() {

     DBCollection collection1 = mock(DBCollection.class);
     DBCollection collection2 = mock(DBCollection.class);

     BasicDBObject value = new BasicDBObject();
     when(collection1.findOne(anyObject())).thenReturn(eq(value));
     //when(collection2.findOne(any(DBObject.class))).thenReturn(eq(value));

     when(db.getCollection("collection1")).thenReturn(collection1);
     when(db.getCollection("collection2")).thenReturn(collection2);


     MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("test"));

     boolean equal = mongoOperation.compareExpectedData(DATA);
     assertThat(equal, is(true));

     }

     @Test
     public void different_data_on_compare_operation_should_assert_expected_data_with_inserted() {

     DBCollection collection1 = mock(DBCollection.class);
     DBCollection collection2 = mock(DBCollection.class);

     when(db.getCollection("collection1")).thenReturn(collection1);
     when(db.getCollection("collection2")).thenReturn(collection2);

     when(collection1.findOne(any(DBObject.class))).thenReturn(null);
     when(collection2.findOne(any(DBObject.class))).thenReturn(eq(new BasicDBObject()));

     MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("test"));

     boolean equal = mongoOperation.compareExpectedData(DATA);
     assertThat(equal, is(false));
     }*/
    private void verifyIndexCreationCommand(Document indexDocument, MongoCollection collection) {

        final ArgumentCaptor<Document> indexCommandCaptor = ArgumentCaptor
                .forClass(Document.class);
        verify(collection, times(1)).createIndex(indexCommandCaptor.capture());
        assertThat(indexCommandCaptor.getValue(), is(indexDocument));

    }

    private void verifyIndexCreationCommandAndOptions(Document indexDocument, IndexOptions indexOptions, MongoCollection collection) {

        final ArgumentCaptor<Document> indexCommandCaptor = ArgumentCaptor
                .forClass(Document.class);
        final ArgumentCaptor<IndexOptions> indexOptionCommandCaptor = ArgumentCaptor
                .forClass(IndexOptions.class);

        verify(collection, times(1)).createIndex(indexCommandCaptor.capture(), indexOptionCommandCaptor.capture());
        assertThat(indexCommandCaptor.getValue(), is(indexDocument));
        assertThat(indexOptionCommandCaptor.getValue().isUnique(), is(indexOptions.isUnique()));

    }

    private void verifyEnableShardingCommand(String expectedCommand, MongoDatabase mockDb) {

        final ArgumentCaptor<Document> enableShardingCommandCaptor = ArgumentCaptor
                .forClass(Document.class);
        verify(mockDb, times(1)).runCommand(enableShardingCommandCaptor.capture());

        Document command = enableShardingCommandCaptor.getValue();

        String commandDocument = JSON.serialize(command);
        assertThat(commandDocument, is(expectedCommand));

    }

    private void verifyInsertedData(String[] expectedData, MongoCollection mockCollection) {

        final ArgumentCaptor<Document> insertCaptor = ArgumentCaptor
                .forClass(Document.class);

        verify(mockCollection, times(2)).insertOne(insertCaptor.capture());

        List<Document> allValues = insertCaptor.getAllValues();
        for (int i = 0; i < expectedData.length; i++) {
            assertThat(allValues.get(i).toJson(), is(expectedData[i]));
        }

    }

}
