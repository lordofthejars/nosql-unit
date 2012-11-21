package com.lordofthejars.nosqlunit.cassandra;

import org.junit.rules.ExternalResource;

public class ManagedCassandra extends ExternalResource {

	public ManagedCassandra() {
		super();
	}

	protected ManagedCassandraLifecycleManager managedCassandraLifecycleManager;
	
	public static class ManagedCassandraRuleBuilder {

		private ManagedCassandraLifecycleManager managedCassandraLifecycleManager;

		private ManagedCassandraRuleBuilder() {
			this.managedCassandraLifecycleManager = new ManagedCassandraLifecycleManager();
		}

		public static ManagedCassandraRuleBuilder newManagedCassandraRule() {
			return new ManagedCassandraRuleBuilder();
		}

		public ManagedCassandraRuleBuilder port(int port) {
			this.managedCassandraLifecycleManager.setPort(port);
			return this;
		}

		public ManagedCassandraRuleBuilder targetPath(String targetPath) {
			this.managedCassandraLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public ManagedCassandraRuleBuilder cassandraPath(String cassandraPath) {
			this.managedCassandraLifecycleManager.setCassandraPath(cassandraPath);
			return this;
		}

		public ManagedCassandraRuleBuilder appendCommandLineArguments(String argumentName, String argumentValue) {
			this.managedCassandraLifecycleManager.addExtraCommandLineArgument(argumentName, argumentValue);
			return this;
		}

		public ManagedCassandraRuleBuilder appendSingleCommandLineArguments(String argument) {
			this.managedCassandraLifecycleManager.addSingleCommandLineArgument(argument);
			return this;
		}

		public ManagedCassandra build() {
			if (this.managedCassandraLifecycleManager.getCassandraPath() == null) {
				throw new IllegalArgumentException("Cassandra Path cannot be null.");
			}

			ManagedCassandra managedCassandra = new ManagedCassandra();
			managedCassandra.managedCassandraLifecycleManager = this.managedCassandraLifecycleManager;
			
			return managedCassandra;
		}

	}

	@Override
	public void before() throws Throwable {
		this.managedCassandraLifecycleManager.startEngine();
	}

	@Override
	public void after() {
		this.managedCassandraLifecycleManager.stopEngine();
	}

	public ManagedCassandraLifecycleManager getManagedCassandraLifecycleManager() {
		return managedCassandraLifecycleManager;
	}
	
	
}
