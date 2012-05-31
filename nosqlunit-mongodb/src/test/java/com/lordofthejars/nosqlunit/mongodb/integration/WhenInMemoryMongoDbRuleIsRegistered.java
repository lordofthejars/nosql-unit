package com.lordofthejars.nosqlunit.mongodb.integration;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.util.concurrent.TimeUnit;

import jmockmongo.MockMongo;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDb;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class WhenInMemoryMongoDbRuleIsRegistered {

	@Test
	public void embedded_mongodb_should_be_started_and_stopped() throws Throwable {

		InMemoryMongoDb inMemoryMongoDb = new InMemoryMongoDb();

		Statement noStatement = new Statement() {

			@Override
			public void evaluate() throws Throwable {
				Mongo mongo = new Mongo("0.0.0.0", MockMongo.DEFAULT_PORT);
				mongo.getDB("test").getCollection("test").insert(new BasicDBObject("name", "Alex"));
				mongo.close();
				
				TimeUnit.SECONDS.sleep(3);
				
				Mongo mongoQuery = new Mongo("0.0.0.0", MockMongo.DEFAULT_PORT);
				DBObject nameDbObject = mongoQuery.getDB("test").getCollection("test").findOne(new BasicDBObject("name", "Alex"));
				assertThat((String)nameDbObject.get("name"), is("Alex"));
				
			}
		};
		
		Statement decotedStatement = inMemoryMongoDb.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();

	}

}
