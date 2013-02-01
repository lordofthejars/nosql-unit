package com.lordofthejars.nosqlunit.mongodb;


public class ManagedMongoDbLifecycleManagerBuilder {

		private ManagedMongoDbLifecycleManager managedMongoDbLifecycleManager;
		
		private ManagedMongoDbLifecycleManagerBuilder() {
			this.managedMongoDbLifecycleManager = new ManagedMongoDbLifecycleManager();
		}

		public static ManagedMongoDbLifecycleManagerBuilder newManagedMongoDbLifecycle() {
			return new ManagedMongoDbLifecycleManagerBuilder();
		}

		public ManagedMongoDbLifecycleManagerBuilder replicaSetName(String replicaSetName) {
			this.managedMongoDbLifecycleManager.setReplicaSetName(replicaSetName);
			return this;
		}
		
		public ManagedMongoDbLifecycleManagerBuilder mongodPath(String mongodPath) {
			this.managedMongoDbLifecycleManager.setMongodPath(mongodPath);
			return this;
		}

		public ManagedMongoDbLifecycleManagerBuilder port(int port) {
			this.managedMongoDbLifecycleManager.setPort(port);
			return this;
		}

		public ManagedMongoDbLifecycleManagerBuilder journaling() {
			this.managedMongoDbLifecycleManager.setJournaling(true);
			return this;
		}
		
		public ManagedMongoDbLifecycleManagerBuilder targetPath(String targetPath) {
			this.managedMongoDbLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public ManagedMongoDbLifecycleManagerBuilder dbRelativePath(String dbRelativePath) {
			this.managedMongoDbLifecycleManager.setDbRelativePath(dbRelativePath);
			return this;
		}

		public ManagedMongoDbLifecycleManagerBuilder logRelativePath(String logRelativePath) {
			this.managedMongoDbLifecycleManager.setLogRelativePath(logRelativePath);
			return this;
		}

		public ManagedMongoDbLifecycleManagerBuilder shardServer() {
			this.managedMongoDbLifecycleManager.setShardServer(true);
			return this;
		}
		
		public ManagedMongoDbLifecycleManagerBuilder configServer() {
			this.managedMongoDbLifecycleManager.setConfigServer(true);
			return this;
		}
		
		public ManagedMongoDbLifecycleManagerBuilder appendCommandLineArguments(
				String argumentName, String argumentValue) {
			this.managedMongoDbLifecycleManager.addExtraCommandLineArgument(argumentName,
					argumentValue);
			return this;
		}

		public ManagedMongoDbLifecycleManagerBuilder appendSingleCommandLineArguments(
				String argument) {
			this.managedMongoDbLifecycleManager.addSingleCommandLineArgument(argument);
			return this;
		}

		
		public ManagedMongoDbLifecycleManager get() {
			if (this.managedMongoDbLifecycleManager.getMongodPath() == null) {
				throw new IllegalArgumentException(
						"No Path to MongoDb is provided.");
			}
			
			return this.managedMongoDbLifecycleManager;
			
		}
}
