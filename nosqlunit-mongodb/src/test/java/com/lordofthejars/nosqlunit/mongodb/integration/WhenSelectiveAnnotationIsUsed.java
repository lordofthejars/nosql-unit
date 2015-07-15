package com.lordofthejars.nosqlunit.mongodb.integration;

import com.lordofthejars.nosqlunit.annotation.Selective;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.*;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class WhenSelectiveAnnotationIsUsed {

	@ClassRule
	public static ManagedMongoDb managedMongoDb1 = newManagedMongoDbRule()
			.mongodPath("/opt/mongo").logRelativePath("log1")
			.dbRelativePath("mongo-dbpath1").targetPath("target/mongo-temp1")
			.port(27017).build();

	@ClassRule
	public static ManagedMongoDb managedMongoDb2 = newManagedMongoDbRule()
			.mongodPath("/opt/mongo").logRelativePath("log2")
			.dbRelativePath("mongo-dbpath2").targetPath("target/mongo-temp2")
			.port(27017 + 1).build();

	@Rule
	public MongoDbRule remoteMongoDbRule1 = new MongoDbRule(mongoDb()
			.databaseName("test").connectionIdentifier("one").port(27017)
			.build(), this);

	@Rule
	public MongoDbRule remoteMongoDbRule2 = new MongoDbRule(mongoDb()
			.databaseName("test").connectionIdentifier("two")
			.port(27017 + 1).build(), this);

	@Test
	@UsingDataSet(withSelectiveLocations = {
			@Selective(identifier = "one", locations = "json.test"),
			@Selective(identifier = "two", locations = "json3.test") }, 
			loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	public void data_should_be_inserted_into_configured_backend() throws UnknownHostException, MongoException {
		
		MongoClient mongo1 = new MongoClient("127.0.0.1", 27017);
    		MongoClient mongo2 = new MongoClient("127.0.0.1", 27017+1);
		
		DB db1 = mongo1.getDB("test");
		DB db2 = mongo2.getDB("test");
		
		DBCollection collection1 = db1.getCollection("collection1");
		
		DBObject foundObject11 = collection1.findOne(buildDbObject("id", 1, "code", "JSON dataset"));
		assertThat(foundObject11, notNullValue());
		DBObject foundObject12 = collection1.findOne(buildDbObject("id", 2, "code", "Another row"));
		assertThat(foundObject12, notNullValue());
		
		DBCollection collection2 = db1.getCollection("collection2");
		
		DBObject foundObject21 = collection2.findOne(buildDbObject("id", 3, "code", "JSON dataset 2"));
		assertThat(foundObject21, notNullValue());
		
		DBObject foundObject22 = collection2.findOne(buildDbObject("id", 4, "code", "Another row 2"));
		assertThat(foundObject22, notNullValue());
		
		DBCollection collection3 = db2.getCollection("collection1");
		
		DBObject foundObject31 = collection3.findOne(buildDbObject("id", 1, "code", "JSON dataset"));
		assertThat(foundObject31, notNullValue());
		
		DBObject foundObject32 = collection3.findOne(buildDbObject("id", 9, "code", "Another row 9"));
		assertThat(foundObject32, notNullValue());
	}

	private DBObject buildDbObject(String idName, int id, String codeName, String code) {
		
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(idName, id);
		params.put(codeName, code);
		
		DBObject dbObject = new BasicDBObject(params);
		
		return dbObject;
	}
	
}
