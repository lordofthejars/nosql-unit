package com.lordofthejars.nosqlunit.mongodb.shard;


public class ManagedMongosLifecycleManagerBuilder {

	private static final String LOCALHOST = "localhost";

	private static final String HOST_PORT_SEPARATOR = ":";
	
	private ManagedMongosLifecycleManager managedMongosLifecycleManager;
	
	private ManagedMongosLifecycleManagerBuilder() {
		this.managedMongosLifecycleManager = new ManagedMongosLifecycleManager();
	}

	public static ManagedMongosLifecycleManagerBuilder newManagedMongosLifecycle() {
		return new ManagedMongosLifecycleManagerBuilder();
	}

	
	public ManagedMongosLifecycleManagerBuilder mongosPath(String mongodPath) {
		this.managedMongosLifecycleManager.setMongosPath(mongodPath);
		return this;
	}

	public ManagedMongosLifecycleManagerBuilder port(int port) {
		this.managedMongosLifecycleManager.setPort(port);
		return this;
	}

	public ManagedMongosLifecycleManagerBuilder logRelativePath(String logRelativePath) {
		this.managedMongosLifecycleManager.setLogRelativePath(logRelativePath);
		return this;
	}
	
	public ManagedMongosLifecycleManagerBuilder configServer(int port) {
		this.managedMongosLifecycleManager.addConfigurationDatabase(LOCALHOST+HOST_PORT_SEPARATOR+Integer.toString(port));
		return this;
	}
	
	public ManagedMongosLifecycleManagerBuilder configServer(String host, int port) {
		this.managedMongosLifecycleManager.addConfigurationDatabase(host+HOST_PORT_SEPARATOR+Integer.toString(port));
		return this;
	}
	
	public ManagedMongosLifecycleManagerBuilder appendCommandLineArguments(
			String argumentName, String argumentValue) {
		this.managedMongosLifecycleManager.addExtraCommandLineArgument(argumentName,
				argumentValue);
		return this;
	}

	public ManagedMongosLifecycleManagerBuilder appendSingleCommandLineArguments(
			String argument) {
		this.managedMongosLifecycleManager.addSingleCommandLineArgument(argument);
		return this;
	}
	
	public ManagedMongosLifecycleManager get() {
		if (this.managedMongosLifecycleManager.getMongosPath() == null) {
			throw new IllegalArgumentException(
					"No Path to Mongos is provided.");
		}
		
		if(!this.managedMongosLifecycleManager.areConfigDatabasesDefined()) {
			throw new IllegalArgumentException("At least one config server should be provided.");
		}
		
		return this.managedMongosLifecycleManager;
		
	}
}

