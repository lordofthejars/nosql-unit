package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoException;

import static com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDbConfigurationBuilder.inMemoryMongoDb;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;


public class MongoDbRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	protected DatabaseOperation<MongoClient> databaseOperation;
	
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
		
		public MongoDbRule defaultEmbeddedMongoDb(String databaseName) {
			return new MongoDbRule(inMemoryMongoDb().databaseName(databaseName).build());
		}
		
		/**
		 * We can use defaultEmbeddedMongoDb(String databaseName).
		 * @param databaseName
		 * @param target
		 * @return
		 */
		@Deprecated
		public MongoDbRule defaultEmbeddedMongoDb(String databaseName, Object target) {
			return new MongoDbRule(inMemoryMongoDb().databaseName(databaseName).build(), target);
		}
		
		public MongoDbRule defaultManagedMongoDb(String databaseName) {
			return new MongoDbRule(mongoDb().databaseName(databaseName).build());
		}
		
		public MongoDbRule defaultManagedMongoDb(String databaseName, int port) {
			return new MongoDbRule(mongoDb().databaseName(databaseName).port(port).build());
		}
		
		public MongoDbRule defaultSpringMongoDb(String databaseName) {
			return new SpringMongoDbRule(mongoDb().databaseName(databaseName).build());
		}
		
		/**
		 * We can use defaultManagedMongoDb(String databaseName).
		 * @param databaseName
		 * @param target
		 * @return
		 */
		@Deprecated
		public MongoDbRule defaultManagedMongoDb(String databaseName, Object target) {
			return new MongoDbRule(mongoDb().databaseName(databaseName).build(), target);
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
			databaseOperation = new MongoOperation(mongoDbConfiguration);
		} catch (MongoException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	/*With JUnit 10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is apporach is the only way.*/
	public MongoDbRule(MongoDbConfiguration mongoDbConfiguration, Object target) {
		super(mongoDbConfiguration.getConnectionIdentifier());
		try {
			setTarget(target);
			databaseOperation = new MongoOperation(mongoDbConfiguration);
		} catch (MongoException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public DatabaseOperation<MongoClient> getDatabaseOperation() {
		return this.databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

	@Override
	public void close() {
		this.databaseOperation.connectionManager().close();
	}

}
