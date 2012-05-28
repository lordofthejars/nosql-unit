package com.lordofthejars.nosqlunit.mongodb;

import java.net.UnknownHostException;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.mongodb.Mongo;
import com.mongodb.MongoException;


public class MongoDbRule extends AbstractNoSqlTestRule {

	private DatabaseOperation databaseOperation;
	
	public MongoDbRule(Class<?> resourceBase, MongoDbConfiguration mongoDbConfiguration) {
		super(resourceBase);
		try {
			databaseOperation = new MongoOperation(new Mongo(mongoDbConfiguration.getHost(), mongoDbConfiguration.getPort()), mongoDbConfiguration);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException(e);
		} catch (MongoException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	protected DatabaseOperation getDatabaseOperation() {
		return this.databaseOperation;
	}
	
}
