package com.lordofthejars.nosqlunit.mongodb.integration;

import com.lordofthejars.nosqlunit.annotation.Selective;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
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

		MongoClient mongo1 = MongoClients.create(MongoClientSettings
				.builder()
				.applyToClusterSettings(b -> b.hosts(Arrays.asList(new ServerAddress("127.0.0.1", 27017))))
				.build());

		MongoClient mongo2 = MongoClients.create(MongoClientSettings
				.builder()
				.applyToClusterSettings(b -> b.hosts(Arrays.asList(new ServerAddress("127.0.0.1", 27017+1))))
				.build());

		
		MongoDatabase db1 = mongo1.getDatabase("test");
		MongoDatabase db2 = mongo2.getDatabase("test");
		
		MongoCollection<Document> collection1 = db1.getCollection("collection1");
		
		Document foundObject11 = collection1.find(buildDbObject("id", 1, "code", "JSON dataset"))
				.first();
		assertThat(foundObject11, notNullValue());
		Document foundObject12 = collection1.find(buildDbObject("id", 2, "code", "Another row"))
				.first();
		assertThat(foundObject12, notNullValue());
		
		MongoCollection<Document> collection2 = db1.getCollection("collection2");
		
		Document foundObject21 = collection2.find(buildDbObject("id", 3, "code", "JSON dataset 2"))
				.first();
		assertThat(foundObject21, notNullValue());
		
		Document foundObject22 = collection2.find(buildDbObject("id", 4, "code", "Another row 2")).first();
		assertThat(foundObject22, notNullValue());
		
		MongoCollection<Document> collection3 = db2.getCollection("collection1");
		
		Document foundObject31 = collection3.find(buildDbObject("id", 1, "code", "JSON dataset"))
				.first();
		assertThat(foundObject31, notNullValue());
		
		Document foundObject32 = collection3.find(buildDbObject("id", 9, "code", "Another row 9"))
				.first();
		assertThat(foundObject32, notNullValue());
	}

	private Document buildDbObject(String idName, int id, String codeName, String code) {
		
		Map<String, Object> params = new HashMap<String, Object>();
		params.put(idName, id);
		params.put(codeName, code);
		
		Document dbObject = new Document(params);
		
		return dbObject;
	}
	
}
