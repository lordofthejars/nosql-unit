package com.lordofthejars.nosqlunit.mongodb;

import java.net.UnknownHostException;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.mongodb.Mongo;
import com.mongodb.MongoException;


public class MongoDbRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	private DatabaseOperation<Mongo> databaseOperation;
	
	public static class MongoDbRuleBuilder {
		
		private MongoDbConfiguration mongoDbConfiguration;
		private Object target;
		
		private MongoDbRuleBuilder() {
		}
		
		public static MongoDbRuleBuilder newMongoDbRule() {
			return new MongoDbRuleBuilder();
		}
		
		public MongoDbRuleBuilder configure(MongoDbConfiguration mongoDbConfiguration) {
			this.mongoDbConfiguration = mongoDbConfiguration;
			return this;
		}
		
		public MongoDbRuleBuilder unitInstance(Object target) {
			this.target = target;
			return this;
		}
		
		public MongoDbRule build() {
			
			if(this.mongoDbConfiguration == null) {
				throw new IllegalArgumentException("Configuration object should be provided.");
			}
			
			return new MongoDbRule(mongoDbConfiguration, target);
		}
		
	}
	
	public MongoDbRule(MongoDbConfiguration mongoDbConfiguration) {
		super(mongoDbConfiguration.getConnectionIdentifier());
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
		super(mongoDbConfiguration.getConnectionIdentifier());
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
	public DatabaseOperation<Mongo> getDatabaseOperation() {
		return this.databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}
	
}
