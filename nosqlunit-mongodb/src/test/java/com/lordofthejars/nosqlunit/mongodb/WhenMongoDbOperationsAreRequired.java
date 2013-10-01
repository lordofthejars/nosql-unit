package com.lordofthejars.nosqlunit.mongodb;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class WhenMongoDbOperationsAreRequired {

	private static final String DATA = "" +
			"{" +
			"\"collection1\": " +
			"	[" +
			"		{\"id\":1,\"code\":\"JSON dataset\",}," +
			"		{\"id\":2,\"code\":\"Another row\",}" +
			"	]," +
			"\"collection2\": " +
			"	[" +
			"		{\"id\":3,\"code\":\"JSON dataset 2\",}," +
			"		{\"id\":4,\"code\":\"Another row 2\",}" +
			"	]" +
			"}";
	
	private static final String DATA_SHARD = "" +
			"{" +
			"\"collection1\": {" +
			"  \"shard-key-pattern\":[\"id\",\"code\"],"+
			"	\"data\":"+
			"				[" +
			"					{\"id\":1,\"code\":\"JSON dataset\",}," +
			"					{\"id\":2,\"code\":\"Another row\",}" +
			"				]"+
			"   }"+
			"}";
	
	private static final String DATA_INDEX = "" +
			"{" +
			"\"collection1\": {" +
			"	\"indexes\": [\n" + 
			"					{\n" + 
			"						\"index\": {\"field\": 1}\n" + 
			"					}\n" + 
			"				 ],"+
			"	\"data\":"+
			"				[" +
			"					{\"id\":1,\"code\":\"JSON dataset\",}," +
			"					{\"id\":2,\"code\":\"Another row\",}" +
			"				]"+
			"   }"+
			"}";
	
	private static final String DATA_INDEX_OPTIONS = "" +
			"{" +
			"\"collection1\": {" +
			"	\"indexes\": [\n" + 
			"					{\n" + 
			"						\"index\": {\"field\": 1},\n" +
			"						\"options\": {\"unique\":\"true\"}  "+		
			"					}\n" + 
			"				 ],"+
			"	\"data\":"+
			"				[" +
			"					{\"id\":1,\"code\":\"JSON dataset\",}," +
			"					{\"id\":2,\"code\":\"Another row\",}" +
			"				]"+
			"   }"+
			"}";
	
	private static final String[] EXPECTED_COLLECTION_1 = new String[]{"{ \"id\" : 1 , \"code\" : \"JSON dataset\"}", "{ \"id\" : 2 , \"code\" : \"Another row\"}"};
	private static final String[] EXPECTED_COLLECTION_2 = new String[]{"{ \"id\" : 3 , \"code\" : \"JSON dataset 2\"}" , "{ \"id\" : 4 , \"code\" : \"Another row 2\"}"};
	
	@Mock private Mongo mongo;
	@Mock private DB db;
	@Mock private DB dbAdmin;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		
		when(mongo.getDB("admin")).thenReturn(dbAdmin);
		when(mongo.getDB("test")).thenReturn(db);
	}
	
	@Test
	public void insert_operation_with_indexes_should_add_data_and_indexes_into_collections() throws UnsupportedEncodingException {
		DBCollection collection1 = mock(DBCollection.class);
		
		when(db.getName()).thenReturn("test");
		when(db.getMongo()).thenReturn(mongo);
		when(db.getCollection("collection1")).thenReturn(collection1);
		
		MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost","test"));
		mongoOperation.insert(new ByteArrayInputStream(DATA_INDEX.getBytes("UTF-8")));
		
		verifyInsertedData(EXPECTED_COLLECTION_1, collection1);
		
		DBObject expectedIndex = new BasicDBObject("field", 1);
		
		verifyIndexCreationCommand(expectedIndex, collection1);
	}
	
	@Test
	public void insert_operation_with_options_in_indexes_should_add_data_and_indexes_into_collections() throws UnsupportedEncodingException {
		DBCollection collection1 = mock(DBCollection.class);
		
		when(db.getName()).thenReturn("test");
		when(db.getMongo()).thenReturn(mongo);
		when(db.getCollection("collection1")).thenReturn(collection1);
		
		MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost","test"));
		mongoOperation.insert(new ByteArrayInputStream(DATA_INDEX_OPTIONS.getBytes("UTF-8")));
		
		verifyInsertedData(EXPECTED_COLLECTION_1, collection1);
		
		DBObject expectedIndex = new BasicDBObject("field", 1);
		DBObject expectedOptionsIndex = new BasicDBObject("unique", "true");
		
		verifyIndexCreationCommandAndOptions(expectedIndex, expectedOptionsIndex, collection1);
	}
	
	@Test
	public void insert_opertation_with_shards_should_add_data_into_collections() throws UnsupportedEncodingException {
	
		DBCollection collection1 = mock(DBCollection.class);
		DBCollection collection2 = mock(DBCollection.class);
		
		when(db.getName()).thenReturn("test");
		when(db.getMongo()).thenReturn(mongo);
		when(db.getCollection("collection1")).thenReturn(collection1);
		when(db.getCollection("collection2")).thenReturn(collection2);
		
		MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost","test"));
		mongoOperation.insert(new ByteArrayInputStream(DATA_SHARD.getBytes("UTF-8")));
		
		verifyInsertedData(EXPECTED_COLLECTION_1, collection1);
		verifyEnableShardingCommand("{ \"shardcollection\" : \"test.collection1\" , \"key\" : { \"id\" : 1 , \"code\" : 1}}", dbAdmin);
	}
	
	@Test
	public void insert_opertation_should_add_data_into_collections() throws UnsupportedEncodingException {
	
		DBCollection collection1 = mock(DBCollection.class);
		DBCollection collection2 = mock(DBCollection.class);
		
		when(db.getCollection("collection1")).thenReturn(collection1);
		when(db.getCollection("collection2")).thenReturn(collection2);
		
		MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost","test"));
		mongoOperation.insert(new ByteArrayInputStream(DATA.getBytes("UTF-8")));
		
		verifyInsertedData(EXPECTED_COLLECTION_1, collection1);
		verifyInsertedData(EXPECTED_COLLECTION_2, collection2);
	}

	@Test
	public void delete_operation_should_remove_all_data_of_collections() {
		
		DBCollection collection1 = mock(DBCollection.class);
		DBCollection collection2 = mock(DBCollection.class);
		
		Set<String> collectionNames = new HashSet<String>();
		collectionNames.add("collection1");
		collectionNames.add("collection2");
		when(db.getCollectionNames()).thenReturn(collectionNames);
		when(db.getCollection("collection1")).thenReturn(collection1);
		when(db.getCollection("collection2")).thenReturn(collection2);
		
		
		MongoOperation mongoOperation = new MongoOperation(mongo, new MongoDbConfiguration("localhost", "test"));
		mongoOperation.deleteAll();
		
		verify(collection1, times(1)).drop();
		verify(collection2, times(1)).drop();
		
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
	
	private void verifyIndexCreationCommand(DBObject indexDocument, DBCollection collection) {
		
		final ArgumentCaptor<DBObject> indexCommandCaptor = ArgumentCaptor
                .forClass(DBObject.class);
		verify(collection, times(1)).ensureIndex(indexCommandCaptor.capture());
		assertThat(indexCommandCaptor.getValue(), is(indexDocument));
		
	}
	
	private void verifyIndexCreationCommandAndOptions(DBObject indexDocument, DBObject indexOptions, DBCollection collection) {
		
		final ArgumentCaptor<DBObject> indexCommandCaptor = ArgumentCaptor
                .forClass(DBObject.class);
		final ArgumentCaptor<DBObject> indexOptionCommandCaptor = ArgumentCaptor
                .forClass(DBObject.class);
		
		verify(collection, times(1)).ensureIndex(indexCommandCaptor.capture(), indexOptionCommandCaptor.capture());
		assertThat(indexCommandCaptor.getValue(), is(indexDocument));
		assertThat(indexOptionCommandCaptor.getValue(), is(indexOptions));
		
	}
	
	private void verifyEnableShardingCommand(String expectedCommand, DB mockDb) {
		
		final ArgumentCaptor<DBObject> enableShardingCommandCaptor = ArgumentCaptor
                .forClass(DBObject.class);
		verify(mockDb, times(1)).command(enableShardingCommandCaptor.capture());
		
		DBObject command = enableShardingCommandCaptor.getValue();

		String commandDocument = JSON.serialize(command);
		assertThat(commandDocument, is(expectedCommand));
		
	}
	
	private void verifyInsertedData(String[] expectedData, DBCollection mockCollection) {
		
		final ArgumentCaptor<DBObject> insertCaptor = ArgumentCaptor
                .forClass(DBObject.class);
		
		verify(mockCollection, times(2)).insert(insertCaptor.capture());
		
		List<DBObject> allValues = insertCaptor.getAllValues();
		for(int i=0;i<expectedData.length;i++) {
			assertThat(allValues.get(i).toString(), is(expectedData[i]));
		}
		
	}
	
}
