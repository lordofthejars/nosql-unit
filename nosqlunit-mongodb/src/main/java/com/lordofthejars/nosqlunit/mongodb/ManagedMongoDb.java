package com.lordofthejars.nosqlunit.mongodb;

import org.junit.rules.ExternalResource;

/**
 * Run a mongodb server before each test suite.
 */
public class ManagedMongoDb extends ExternalResource {

	
	private ManagedMongoDb() {
		super();
	}

	protected ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager;
	
	/**
	 * Builder to start mongodb server accordingly to your setup
	 */
	public static class MongoServerRuleBuilder {

		private ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager;
		
		private MongoServerRuleBuilder() {
			this.managedMongoDbLifecycleManager = new ManagedMongoDbLifecycleManager();
		}

		public static MongoServerRuleBuilder newManagedMongoDbRule() {
			return new MongoServerRuleBuilder();
		}

		public MongoServerRuleBuilder mongodPath(String mongodPath) {
			this.managedMongoDbLifecycleManager.setMongodPath(mongodPath);
			return this;
		}

		public MongoServerRuleBuilder port(int port) {
			this.managedMongoDbLifecycleManager.setPort(port);
			return this;
		}

		public MongoServerRuleBuilder journaling() {
			this.managedMongoDbLifecycleManager.setJournaling(true);
			return this;
		}
		
		public MongoServerRuleBuilder targetPath(String targetPath) {
			this.managedMongoDbLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public MongoServerRuleBuilder dbRelativePath(String dbRelativePath) {
			this.managedMongoDbLifecycleManager.setDbRelativePath(dbRelativePath);
			return this;
		}

		public MongoServerRuleBuilder logRelativePath(String logRelativePath) {
			this.managedMongoDbLifecycleManager.setLogRelativePath(logRelativePath);
			return this;
		}


		public MongoServerRuleBuilder appendCommandLineArguments(
				String argumentName, String argumentValue) {
			this.managedMongoDbLifecycleManager.addExtraCommandLineArgument(argumentName,
					argumentValue);
			return this;
		}

		public MongoServerRuleBuilder appendSingleCommandLineArguments(
				String argument) {
			this.managedMongoDbLifecycleManager.addSingleCommandLineArgument(argument);
			return this;
		}

		
		public ManagedMongoDb build() {
			if (this.managedMongoDbLifecycleManager.getMongodPath() == null) {
				throw new IllegalArgumentException(
						"No Path to MongoDb is provided.");
			}
			
			ManagedMongoDb managedMongoDb = new ManagedMongoDb();
			managedMongoDb.managedMongoDbLifecycleManager = this.managedMongoDbLifecycleManager;
			
			return managedMongoDb;
		}
	}

	@Override
	public void before() throws Throwable {
		this.managedMongoDbLifecycleManager.startEngine();
	}

	@Override
	public void after() {
		this.managedMongoDbLifecycleManager.stopEngine();
	}
	
}
