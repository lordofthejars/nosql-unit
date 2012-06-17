package com.lordofthejars.nosqlunit.mongodb.integration;

import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.inject.Inject;
import javax.inject.Named;

import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.Mongo;

public class WhenMultipleMongoObjectsAreAnnotatedWithInject {

//	@ClassRule
//	public static ManagedMongoDb managedMongoDb1 = newManagedMongoDbRule().mongodPath("/opt/mongo")
//			.build();
	
	@Rule
	public MongoDbRule remoteMongoDbRule1 = new MongoDbRule(mongoDb()
			.databaseName("test").connectionIdentifier("one").build() ,this);
	
	@Rule
	public MongoDbRule remoteMongoDbRule2 = new MongoDbRule(mongoDb()
			.databaseName("test").connectionIdentifier("two").build() ,this);
	
	@Named("one")
	@Inject
	private Mongo mongo1;
	
	@Named("two")
	@Inject
	private Mongo mongo2;
	

	@Test
	public void mongo_instance_used_in_rule_should_be_injected() throws Throwable {
		
		assertThat(mongo1, is(remoteMongoDbRule1.getDatabaseOperation().connectionManager()));
		assertThat(mongo2, is(remoteMongoDbRule2.getDatabaseOperation().connectionManager()));
		
	}
	
}
