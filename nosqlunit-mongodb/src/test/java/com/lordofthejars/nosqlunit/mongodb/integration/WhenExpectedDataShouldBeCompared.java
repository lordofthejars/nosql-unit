package com.lordofthejars.nosqlunit.mongodb.integration;


import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfiguration;
import com.lordofthejars.nosqlunit.mongodb.MongoOperation;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

public class WhenExpectedDataShouldBeCompared {

	private static final String DATA_SHARD = "" +
			"{" +
			"\"collection1\": " +
			"				[" +
			"					{\"id\":1,\"code\":\"JSON dataset\",}," +
			"					{\"id\":2,\"code\":\"Another row\",}" +
			"				]"+
			"}";
	
	private static final String DATA_SHARD_2 = "" +
			"{" +
			"\"collection1\": {" +
			"  \"shard-key-pattern\":[\"id\",\"code2\"],"+
			"	\"data\":"+
			"				[" +
			"					{\"id\":1,\"code\":\"JSON dataset\",}," +
			"					{\"id\":2,\"code\":\"Another row\",}" +
			"				]"+
			"   }"+
			"}";
	
	@ClassRule
	public static ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo")
			.build();
	
	private static MongoOperation mongoOperation;
	
	@BeforeClass
	public static final void startUp() throws UnknownHostException, MongoException {
		MongoDbConfiguration mongoConfiguration = mongoDb().databaseName("test").build();
		mongoOperation = new MongoOperation(mongoConfiguration);
	}
	
	@Before
	public void setUp() {
		MongoDatabase mongoDb = getMongoDB();
		dropDatabase(mongoDb);
	}
	
	@Test
	public void shards_data_should_be_ignored() {
		
		mongoOperation.insert(new ByteArrayInputStream(DATA_SHARD.getBytes()));
		boolean isEquals = mongoOperation.databaseIs(new ByteArrayInputStream(DATA_SHARD_2.getBytes()));
		assertThat(isEquals, is(true));
		
	}
	
	@Test
	public void empty_database_and_empty_expectation_should_be_equals() {
		
		boolean isEquals = mongoOperation.databaseIs(new ByteArrayInputStream("{}".getBytes()));
		
		assertThat(isEquals, is(true));
	}
	
	@Test
	public void empty_expected_collection_and_database_collection_with_content_should_fail() throws UnsupportedEncodingException {
		try {
		
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection has 0 elements but insert collection has 1"));
		}
	}
	
	
	@Test
	public void empty_expected_collection_and_empty_database_collection_should_be_equal() throws UnsupportedEncodingException {
		
		createCollection(getMongoDB(), "col1");
		
		boolean isEquals = mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[]}".getBytes("UTF-8")));
		assertThat(isEquals, is(true));
	}
	
	@Test
	public void empty_expected_collection_and_empty_database_should_fail() throws UnsupportedEncodingException {
		try {
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection names are [col1] but insert collection names are []"));
		}
		
	}
	
	@Test
	public void empty_expectation_and_empty_database_collection_should_fail() throws UnsupportedEncodingException {
		try {
			createCollection(getMongoDB(), "col1");
			mongoOperation.databaseIs(new ByteArrayInputStream("{}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection names are [] but insert collection names are [col1]"));
		}
		
	}
	
	@Test
	public void empty_expected_collection_and_empty_database_collection_with_different_names_should_fail() throws UnsupportedEncodingException {
		try {
			createCollection(getMongoDB(), "col1");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col2\":[]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection names are [col2] but insert collection names are [col1]"));
		}
		
	}
	
	@Test
	public void expected_collection_and_database_collection_with_same_content_should_be_equals() throws UnsupportedEncodingException {
		
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
		
			boolean isEquals = mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
			assertThat(isEquals, is(true));
		
	}
	
	@Test
	public void expected_collection_and_database_collection_with_different_content_should_fail() throws UnsupportedEncodingException {
		try {
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Soto\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Object # { \"name\" : \"Soto\"} # is not found into collection [col1]"));
		}
		
	}
	
	@Test
	public void expected_collection_with_content_and_database_collection_empty_should_fail() throws UnsupportedEncodingException {
		
		try {
			createCollection(getMongoDB(), "col1");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection has 1 elements but insert collection has 0"));
		}
			
	}
	
	@Test
	public void expected_collection_and_database_collection_with_different_names_should_fail() throws UnsupportedEncodingException {
		try {
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col2\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection names are [col2] but insert collection names are [col1]"));
		}
		
	}
	
	@Test
	public void less_expected_collection_than_database_collection_should_fail() throws UnsupportedEncodingException {
		try {
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
			addCollectionWithData(getMongoDB(), "col3", "name", "Alex");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection names are [col1] but insert collection names are [col1, col3]"));
		}
		
	}
	
	@Test
	public void expected_collection_has_some_items_different_than_database_collection_items_should_fail() throws UnsupportedEncodingException {
		try {
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}, {\"name\":\"Soto\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection has 2 elements but insert collection has 1"));
		}
		
	}
	
	@Test
	public void expected_collection_item_has_more_attributes_than_database_collection_item_attributes_should_fail() throws UnsupportedEncodingException {
		try {
			//Inserted one element
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
		
			//Expected with two elements
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\", \"surname\":\"Soto\"}]}".getBytes("UTF-8")));
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Object # { \"name\" : \"Alex\" , \"surname\" : \"Soto\"} # is not found into collection [col1]"));
		}
		
	}
	
	@Test
	public void expected_collection_item_has_same_attributes_as_database_collection_item_attributes_but_different_values_should_fail() throws UnsupportedEncodingException {
		try {
			//Inserted one element
			addCollectionWithTwoData(getMongoDB(), "col1", "name", "Alex","surname", "Sot");
		
			//Expected with two elements
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\", \"surname\":\"Soto\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Object # { \"name\" : \"Alex\" , \"surname\" : \"Soto\"} # is not found into collection [col1]"));
		}
		
	}
	
	@Test
	public void expected_collection_item_has_less_attributes_than_database_collection_item_attributes_should_fail() throws UnsupportedEncodingException {
		try {
			//Inserted one element
			addCollectionWithTwoData(getMongoDB(), "col1", "name", "Alex","surname", "Soto");
		
			//Expected with two elements
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected DbObject and insert DbObject have different keys: Expected: [name] Inserted: [name, surname]"));
		}
		
	}
	
	
	@Test
	public void expected_collection_has_all_items_different_than_database_collection_items_should_fail() throws UnsupportedEncodingException {
		try {
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Soto\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Object # { \"name\" : \"Soto\"} # is not found into collection [col1]"));
		}
		
	}
	
	
	@Test
	public void more_expected_collection_than_database_collection_should_fail() throws UnsupportedEncodingException {
		try {
			addCollectionWithData(getMongoDB(), "col1", "name", "Alex");
		
			mongoOperation.databaseIs(new ByteArrayInputStream("{\"col1\":[{\"name\":\"Alex\"}], \"col3\":[{\"name\":\"Alex\"}]}".getBytes("UTF-8")));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected collection names are [col1, col3] but insert collection names are [col1]"));
		}
		
	}
	
	private void createCollection(MongoDatabase mongoDb, String collectionName) {

		CreateCollectionOptions collectionOptions = new CreateCollectionOptions();
		collectionOptions.maxDocuments(1);

		mongoDb.createCollection(collectionName, collectionOptions);
	}
	
	private void addCollectionWithTwoData(MongoDatabase mongoDb, String collectionName, String field, String value, String field2, String value2) {
		MongoCollection<Document> collection = mongoDb.getCollection(collectionName);
		Map<String, Object> documentParams = new HashMap<>();
		documentParams.put(field, value);
		documentParams.put(field2, value2);
		Document dbObject = new Document(documentParams);
		collection.insertOne(dbObject);
	}
	
	private void addCollectionWithData(MongoDatabase mongoDb, String collectionName, String field, String value) {
		MongoCollection<Document> collection = mongoDb.getCollection(collectionName);
		Document dbObject = new Document(field, value);
		collection.insertOne(dbObject);
	}
	
	private void dropDatabase(MongoDatabase mongoDb) {
		mongoDb.drop();
	}
	
	private MongoDatabase getMongoDB() {
		return mongoOperation.connectionManager().getDatabase("test");
	}
	
}
