package com.lordofthejars.nosqlunit.cassandra;

import org.junit.rules.ExternalResource;

public class EmbeddedCassandra extends ExternalResource {

	
	private EmbeddedCassandra() {
		super();
	}
	
	protected EmbeddedCassandraLifecycleManager embeddedCassandraLifecycleManager;
	
	public static class EmbeddedCassandraRuleBuilder {

		private EmbeddedCassandraLifecycleManager embeddedCassandraLifecycleManager;

		private EmbeddedCassandraRuleBuilder() {
			this.embeddedCassandraLifecycleManager = new EmbeddedCassandraLifecycleManager();
		}

		public static EmbeddedCassandraRuleBuilder newEmbeddedCassandraRule() {
			return new EmbeddedCassandraRuleBuilder();
		}

		public EmbeddedCassandraRuleBuilder targetPath(String targetPath) {
			this.embeddedCassandraLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public EmbeddedCassandraRuleBuilder cassandraConfigurationPath(String cassandraConfigurationPath) {
			this.embeddedCassandraLifecycleManager.setCassandraConfigurationFile(cassandraConfigurationPath);
			return this;
		}
		
		public EmbeddedCassandraRuleBuilder port(int port) {
			this.embeddedCassandraLifecycleManager.setPort(port);
			return this;
		}
		
		public EmbeddedCassandra build() {
			
			if (this.embeddedCassandraLifecycleManager.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Cassandra is provided.");
			}
			
			EmbeddedCassandra embeddedCassandra = new EmbeddedCassandra();
			embeddedCassandra.embeddedCassandraLifecycleManager = this.embeddedCassandraLifecycleManager;
			
			return embeddedCassandra;
		}

	}

	@Override
	public void before() throws Throwable {
		this.embeddedCassandraLifecycleManager.startEngine();
	}

	@Override
	public void after() {
		this.embeddedCassandraLifecycleManager.stopEngine();
	}
	
	
}
