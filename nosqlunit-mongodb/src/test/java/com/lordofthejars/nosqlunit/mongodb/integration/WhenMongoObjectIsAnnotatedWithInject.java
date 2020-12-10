package com.lordofthejars.nosqlunit.mongodb.integration;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;

import java.lang.reflect.Method;

import javax.inject.Inject;

import com.mongodb.client.MongoClient;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfiguration;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;

public class WhenMongoObjectIsAnnotatedWithInject {

	@ClassRule
	public static ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo")
			.build();
	
	@Inject
	private MongoClient mongo;
	
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
		
		FrameworkMethod frameworkMethod = frameworkMethod(WhenMongoObjectIsAnnotatedWithInject.class, "mongo_instance_used_in_rule_should_be_injected");
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, frameworkMethod, this);
		mongodbStatement.evaluate();
		
		assertThat(mongo, is(remoteMongoDbRule.getDatabaseOperation().connectionManager()));
		
	}
	
	@Test
	public void mongo_instance_used_in_rule_should_be_injected_without_this_reference() throws Throwable {
		
		MongoDbConfiguration mongoDbConfiguration = mongoDb()
				.databaseName("test").build();
		MongoDbRule remoteMongoDbRule = new MongoDbRule(mongoDbConfiguration);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				
			}
		};
		
		FrameworkMethod frameworkMethod = frameworkMethod(WhenMongoObjectIsAnnotatedWithInject.class, "mongo_instance_used_in_rule_should_be_injected");
		Statement mongodbStatement = remoteMongoDbRule.apply(noStatement, frameworkMethod, this);
		mongodbStatement.evaluate();
		
		assertThat(mongo, is(remoteMongoDbRule.getDatabaseOperation().connectionManager()));
		
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
