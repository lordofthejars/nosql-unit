package com.lordofthejars.nosqlunit.mongodb;

import java.net.UnknownHostException;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.mongodb.Mongo;
import com.mongodb.MongoException;


public class MongoDbRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	private DatabaseOperation databaseOperation;
	
	public MongoDbRule(MongoDbConfiguration mongoDbConfiguration) {
		super();
		try {
			databaseOperation = new MongoOperation(new Mongo(mongoDbConfiguration.getHost(), mongoDbConfiguration.getPort()), mongoDbConfiguration);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		} catch (MongoException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	/*With JUnit 10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is apporach is the only way.*/
	public MongoDbRule(MongoDbConfiguration mongoDbConfiguration, Object target) {
		super();
		try {
			setTarget(target);
			databaseOperation = new MongoOperation(new Mongo(mongoDbConfiguration.getHost(), mongoDbConfiguration.getPort()), mongoDbConfiguration);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		} catch (MongoException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public DatabaseOperation getDatabaseOperation() {
		return this.databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}
	
}
