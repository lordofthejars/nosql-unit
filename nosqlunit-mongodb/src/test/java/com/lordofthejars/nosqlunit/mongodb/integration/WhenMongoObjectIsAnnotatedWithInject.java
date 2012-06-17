package com.lordofthejars.nosqlunit.mongodb.integration;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;

import javax.inject.Inject;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfiguration;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.Mongo;

public class WhenMongoObjectIsAnnotatedWithInject {

	@ClassRule
	public static ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo")
			.build();
	
	@Inject
	private Mongo mongo;
	
	@Before
	public void setUp() {
		mongo = null;
	}

	@Test
	public void mongo_instance_used_in_rule_should_be_injected() throws Throwable {
		
		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration, this);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		Description description = Description.createTestDescription(WhenMongoObjectIsAnnotatedWithInject.class, "nosqltest");
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, description);
		mongodbStatement.evaluate();
		
		assertThat(mongo, is(remoteMongoDbRule.getDatabaseOperation().connectionManager()));
		
	}
	
}
