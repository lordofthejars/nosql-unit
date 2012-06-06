package com.lordofthejars.nosqlunit.mongodb.integration;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.mongodb.integration.UsingDataSetAnnotationTest;
import com.lordofthejars.nosqlunit.mongodb.integration.ShouldMatchDataSetAnnotationTest;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfiguration;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class WhenMongoDbRuleIsRegistered {

	@ClassRule
	public static ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo")
			.build();
	
	
	@Test(expected=NoSqlAssertionError.class)
	public void should_fail_if_expected_data_is_non_strict_equal() throws Throwable {
		
		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		UsingDataSetAnnotationTest usingDataSetAnnotationTest = new UsingDataSetAnnotationTest(new String[]{"json.test"}, LoadStrategyEnum.CLEAN_INSERT);
		ShouldMatchDataSetAnnotationTest shouldMatchDataSetAnnotationTest = new ShouldMatchDataSetAnnotationTest("json3.test");
		Description description = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", usingDataSetAnnotationTest, shouldMatchDataSetAnnotationTest);
		
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, description);
		mongodbStatement.evaluate();
		
	}
	
	@Test
	public void should_assert_if_expected_data_is_strict_equal() throws Throwable {
		
		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		UsingDataSetAnnotationTest usingDataSetAnnotationTest = new UsingDataSetAnnotationTest(new String[]{"json.test"}, LoadStrategyEnum.CLEAN_INSERT);
		ShouldMatchDataSetAnnotationTest shouldMatchDataSetAnnotationTest = new ShouldMatchDataSetAnnotationTest("json.test");
		Description description = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", usingDataSetAnnotationTest, shouldMatchDataSetAnnotationTest);
		
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, description);
		mongodbStatement.evaluate();
		
	}
	
	@Test
	public void should_insert_only_new_elements_with_refresh_strategy() throws Throwable {
		
		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		UsingDataSetAnnotationTest usingDataSetAnnotationTest = new UsingDataSetAnnotationTest(new String[]{"json.test"}, LoadStrategyEnum.CLEAN_INSERT);
		Description description = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", usingDataSetAnnotationTest);
		
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, description);
		mongodbStatement.evaluate();
		
		UsingDataSetAnnotationTest refreshedDataSetTest = new UsingDataSetAnnotationTest(new String[]{"json3.test"}, LoadStrategyEnum.REFRESH);
		Description refreshedDescription = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", refreshedDataSetTest);
		
		Statement refreshMongodbStatement = remoteMongoDbRule.apply(noStatement, refreshedDescription);
		refreshMongodbStatement.evaluate();
		
		DBObject currentData = findOneDBOjectByParameter("collection1",
				"id", 9);	
		assertThat((String)currentData.get("code"), is("Another row 9"));
		
		int numberOfElementsWithId1 = countDBObjectsByParameter("collection1", "id", 1);
		assertThat(numberOfElementsWithId1, is(1));
		
	}
	
	@Test
	public void should_clean_dataset_with_delete_all_strategy() throws Throwable {
		
		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		UsingDataSetAnnotationTest usingDataSetAnnotationTest = new UsingDataSetAnnotationTest(new String[]{"json.test"}, LoadStrategyEnum.DELETE_ALL);
		Description description = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", usingDataSetAnnotationTest);
		
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, description);
		mongodbStatement.evaluate();
		
		DBObject currentData = findOneDBOjectByParameter("collection1",
				"id", 1);	
		assertThat(currentData, nullValue());
	}
	
	@Test
	public void should_insert_new_dataset_with_insert_strategy() throws Throwable {
		
		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		UsingDataSetAnnotationTest usingDataSetAnnotationTest = new UsingDataSetAnnotationTest(new String[]{"json.test"}, LoadStrategyEnum.INSERT);
		Description description = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", usingDataSetAnnotationTest);
		
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, description);
		mongodbStatement.evaluate();
		
		DBObject currentData = findOneDBOjectByParameter("collection1",
				"id", 1);	
		assertThat((String)currentData.get("code"), is("JSON dataset"));
		
		UsingDataSetAnnotationTest dataSetTest2 = new UsingDataSetAnnotationTest(new String[]{"json2.test"}, LoadStrategyEnum.INSERT);
		Description description2 = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", dataSetTest2);
		
		Statement mongodbStatement2 = remoteMongoDbRule.apply(noStatement, description2);
		mongodbStatement2.evaluate();
		
		DBObject previousData = findOneDBOjectByParameter("collection1",
				"id", 1);	
		assertThat((String)previousData.get("code"), is("JSON dataset"));
		
		DBObject data = findOneDBOjectByParameter("collection3",
				"id", 6);	
		assertThat((String)data.get("code"), is("Another row"));
	}
	
	@Test
	public void should_clean_previous_data_and_insert_new_dataset_with_clean_insert_strategy() throws Throwable {
		
		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		UsingDataSetAnnotationTest usingDataSetAnnotationTest = new UsingDataSetAnnotationTest(new String[]{"json.test"}, LoadStrategyEnum.CLEAN_INSERT);
		Description description = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", usingDataSetAnnotationTest);
		
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, description);
		mongodbStatement.evaluate();
		
		DBObject currentData = findOneDBOjectByParameter("collection1",
				"id", 1);	
		assertThat((String)currentData.get("code"), is("JSON dataset"));
		
		UsingDataSetAnnotationTest dataSetTest2 = new UsingDataSetAnnotationTest(new String[]{"json2.test"}, LoadStrategyEnum.CLEAN_INSERT);
		Description description2 = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", dataSetTest2);
		
		Statement mongodbStatement2 = remoteMongoDbRule.apply(noStatement, description2);
		mongodbStatement2.evaluate();
		
		DBObject previousData = findOneDBOjectByParameter("collection1",
				"id", 1);	
		assertThat(previousData, nullValue());
		
		DBObject data = findOneDBOjectByParameter("collection3",
				"id", 6);	
		assertThat((String)data.get("code"), is("Another row"));
	}
	
	@Test
	public void should_insert_new_dataset_with_clean_insert_strategy() throws Throwable {

		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		UsingDataSetAnnotationTest usingDataSetAnnotationTest = new UsingDataSetAnnotationTest(new String[]{"json.test"}, LoadStrategyEnum.CLEAN_INSERT);
		Description description = Description.createTestDescription(WhenMongoDbRuleIsRegistered.class, "nosqltest", usingDataSetAnnotationTest);
		
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, description);
		mongodbStatement.evaluate();
		
		DBObject data = findOneDBOjectByParameter("collection1",
				"id", 1);	
		assertThat((String)data.get("code"), is("JSON dataset"));

	}

	private int countDBObjectsByParameter(String collectionName, String parameterName, Object value) throws UnknownHostException, MongoException {
		
		Mongo mongo = new Mongo("localhost");
		DB mongodb = mongo.getDB("test");
		DBCollection collection = mongodb.getCollection(collectionName);
		
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put(parameterName, value);
		
		BasicDBObject basicDBObject = new BasicDBObject(parameters);
		
		DBCursor cursor = collection.find(basicDBObject);
		
		return cursor.count();
	}
	
	private DBObject findOneDBOjectByParameter(String collectionName,
			String parameterName, Object value) throws UnknownHostException {
		Mongo mongo = new Mongo("localhost");
		DB mongodb = mongo.getDB("test");
		DBCollection collection = mongodb.getCollection(collectionName);
		
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put(parameterName, value);
		
		BasicDBObject basicDBObject = new BasicDBObject(parameters);
		DBObject data = collection.findOne(basicDBObject);
		return data;
	}
}
