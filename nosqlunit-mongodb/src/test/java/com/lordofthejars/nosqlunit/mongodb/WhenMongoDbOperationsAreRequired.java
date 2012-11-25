package com.lordofthejars.nosqlunit.mongodb;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
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

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfiguration;
import com.lordofthejars.nosqlunit.mongodb.MongoOperation;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

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
	
	private static final String[] EXPECTED_COLLECTION_1 = new String[]{"{ \"id\" : 1 , \"code\" : \"JSON dataset\"}", "{ \"id\" : 2 , \"code\" : \"Another row\"}"};
	private static final String[] EXPECTED_COLLECTION_2 = new String[]{"{ \"id\" : 3 , \"code\" : \"JSON dataset 2\"}" , "{ \"id\" : 4 , \"code\" : \"Another row 2\"}"};
	
	@Mock private Mongo mongo;
	@Mock private DB db;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		
		when(mongo.getDB(anyString())).thenReturn(db);
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
