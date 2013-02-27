package com.lordofthejars.nosqlunit.mongodb.integration;

import static com.lordofthejars.nosqlunit.mongodb.MongoDbRule.MongoDbRuleBuilder.newMongoDbRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.Mongo;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations="embedded-mongo-spring-definition.xml")
public class WhenSpringEmbeddedInstanceIsRequired {

	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private Mongo mongo;
	
	@Rule
	public MongoDbRule mongoDbRule = newMongoDbRule().defaultSpringMongoDb("test");
	
	@Test
	public void connection_manager_should_be_the_one_defined_in_application_context() {
		
		DatabaseOperation<Mongo> databaseOperation = mongoDbRule.getDatabaseOperation();
		Mongo connectionManager = databaseOperation.connectionManager();
		
		assertThat(connectionManager, is(mongo));
		
	}
	
}
