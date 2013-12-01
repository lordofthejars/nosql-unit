package com.lordofthejars.nosqlunit.mongodb.integration;

import com.mongodb.MongoClient;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.mongodb.CommandResult;
import com.mongodb.DB;

public class WhenManagedMongoDbRuleIsRegistered {

	@Test
	public void mongo_server_should_start_and_stop_from_mongo_home() throws Throwable {
		
		System.setProperty("MONGO_HOME", "/opt/mongo");
		
		ManagedMongoDb managedMongoDb = newManagedMongoDbRule()
				.build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				MongoClient server = new MongoClient();
				DB db = server.getDB("admin");
				CommandResult stats = db.getStats();
				assertThat(stats.ok(), is(true));
			}
		};
		
		Statement decotedStatement = managedMongoDb.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File("target"
				+ File.separatorChar + "mongo-temp"+ File.separatorChar + "mongo-dbpath");
		assertThat(dbPath.exists(), is(false));
		
		System.clearProperty("MONGO_HOME");
		
	}
	
	@Test
	public void mongo_server_should_start_and_stop_from_configured_location() throws Throwable {
		
		ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo")
				.build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				MongoClient server = new MongoClient();
				DB db = server.getDB("admin");
				CommandResult stats = db.getStats();
				assertThat(stats.ok(), is(true));
			}
		};
		
		Statement decotedStatement = managedMongoDb.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File dbPath = new File("target"
				+ File.separatorChar + "mongo-temp"+ File.separatorChar + "mongo-dbpath");
		assertThat(dbPath.exists(), is(false));
		
	}
	
	
	@Test(expected=IllegalArgumentException.class)
	public void mongo_server_should_throw_an_exception_if_mongo_location_is_not_set() throws Throwable {
		
		ManagedMongoDb managedMongoDb = newManagedMongoDbRule()
				.build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		Statement decotedStatement = managedMongoDb.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
	}
	
	@Test(expected=IllegalStateException.class)
	public void mongo_server_should_throw_an_exception_if_mongo_location_is_not_found() throws Throwable {
		
		ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/example")
				.build();
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		Statement decotedStatement = managedMongoDb.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
	}
	
}
