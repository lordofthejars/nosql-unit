package com.lordofthejars.nosqlunit.mongodb.integration;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;
import com.mongodb.MongoClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.mongodb.integration.UsingDataSetAnnotationTest;
import com.lordofthejars.nosqlunit.mongodb.integration.ShouldMatchDataSetAnnotationTest;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
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
	public static ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo").build();

	@Test(expected = NoSqlAssertionError.class)
	public void should_fail_if_expected_data_is_non_strict_equal() throws Throwable {

		MongoDbConfiguration mongoDbConfiguration = mongoDb().databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);

		Statement noStatement = new Statement() {

			@Override
			public void evaluate() throws Throwable {

			}
		};


		FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_wrong_test");

		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
		mongodbStatement.evaluate();

	}

	@Test
	public void should_assert_if_expected_data_is_strict_equal() throws Throwable {

		MongoDbConfiguration mongoDbConfiguration = mongoDb().databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);

		Statement noStatement = new Statement() {

			@Override
			public void evaluate() throws Throwable {

			}
		};

		FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_equal_test");

		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
		mongodbStatement.evaluate();

	}

	@Test
	public void should_clean_dataset_with_delete_all_strategy() throws Throwable {

		MongoDbConfiguration mongoDbConfiguration = mongoDb().databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);

		Statement noStatement = new Statement() {

			@Override
			public void evaluate() throws Throwable {

			}
		};

		FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_delete_test");

		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, frameworkMethod, new MyTestClass());
		mongodbStatement.evaluate();

		DBObject currentData = findOneDBOjectByParameter("collection1", "id", 1);
		assertThat(currentData, nullValue());
	}

	@Test
	public void should_insert_new_dataset_with_insert_strategy() throws Throwable {

		MongoDbConfiguration mongoDbConfiguration = mongoDb().databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);

		Statement noStatement = new Statement() {

			@Override
			public void evaluate() throws Throwable {

			}
		};

		FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_insert_test_1");

		MyTestClass testObject = new MyTestClass();
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, frameworkMethod, testObject);
		mongodbStatement.evaluate();

		DBObject currentData = findOneDBOjectByParameter("collection1", "id", 1);
		assertThat((String) currentData.get("code"), is("JSON dataset"));

		FrameworkMethod frameworkMethod2 = frameworkMethod(MyTestClass.class, "my_insert_test_2");

		Statement mongodbStatement2 = remoteMongoDbRule.apply(noStatement, frameworkMethod2, testObject);
		mongodbStatement2.evaluate();

		DBObject previousData = findOneDBOjectByParameter("collection1", "id", 1);
		assertThat((String) previousData.get("code"), is("JSON dataset"));

		DBObject data = findOneDBOjectByParameter("collection3", "id", 6);
		assertThat((String) data.get("code"), is("Another row"));
	}

	@Test
	public void should_clean_previous_data_and_insert_new_dataset_with_clean_insert_strategy() throws Throwable {

		MongoDbConfiguration mongoDbConfiguration = mongoDb().databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);

		Statement noStatement = new Statement() {

			@Override
			public void evaluate() throws Throwable {

			}
		};

		FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_equal_test");

		MyTestClass testObject = new MyTestClass();
		
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, frameworkMethod, testObject);
		mongodbStatement.evaluate();

		DBObject currentData = findOneDBOjectByParameter("collection1", "id", 1);
		assertThat((String) currentData.get("code"), is("JSON dataset"));

		FrameworkMethod frameworkMethod2 = frameworkMethod(MyTestClass.class, "my_equal_test_2");

		Statement mongodbStatement2 = remoteMongoDbRule.apply(noStatement, frameworkMethod2, testObject);
		mongodbStatement2.evaluate();

		DBObject previousData = findOneDBOjectByParameter("collection1", "id", 1);
		assertThat(previousData, nullValue());

		DBObject data = findOneDBOjectByParameter("collection3", "id", 6);
		assertThat((String) data.get("code"), is("Another row"));
	}


	private int countDBObjectsByParameter(String collectionName, String parameterName, Object value)
			throws UnknownHostException, MongoException {

		MongoClient mongo = new MongoClient("localhost");
		DB mongodb = mongo.getDB("test");
		DBCollection collection = mongodb.getCollection(collectionName);

		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put(parameterName, value);

		BasicDBObject basicDBObject = new BasicDBObject(parameters);

		DBCursor cursor = collection.find(basicDBObject);

		return cursor.count();
	}

	private DBObject findOneDBOjectByParameter(String collectionName, String parameterName, Object value)
			throws UnknownHostException {
		MongoClient mongo = new MongoClient("localhost");
		DB mongodb = mongo.getDB("test");
		DBCollection collection = mongodb.getCollection(collectionName);

		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put(parameterName, value);

		BasicDBObject basicDBObject = new BasicDBObject(parameters);
		DBObject data = collection.findOne(basicDBObject);
		return data;
	}

	private FrameworkMethod frameworkMethod(Class<?> testClass, String methodName) {

		try {
			Method method = testClass.getMethod(methodName);
			return new FrameworkMethod(method);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		}

	}
}

class MyTestClass {

	@Test
	@UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	@ShouldMatchDataSet(location = "json3.test")
	public void my_wrong_test() {
	}
	
	@Test
	@UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	@ShouldMatchDataSet(location = "json.test")
	public void my_equal_test() {
	}
	
	@Test
	@UsingDataSet(locations = "json2.test", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	public void my_equal_test_2() {
	}
	
	@Test
	@UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.DELETE_ALL)
	public void my_delete_test() {
	}
	
	
	@Test
	@UsingDataSet(locations = "json.test", loadStrategy = LoadStrategyEnum.INSERT)
	public void my_insert_test_1() {
	}
	
	@Test
	@UsingDataSet(locations = "json2.test", loadStrategy = LoadStrategyEnum.INSERT)
	public void my_insert_test_2() {
	}

}
