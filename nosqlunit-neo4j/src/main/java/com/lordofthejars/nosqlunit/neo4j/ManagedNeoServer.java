package com.lordofthejars.nosqlunit.neo4j;

import org.junit.rules.ExternalResource;

public class ManagedNeoServer extends ExternalResource {

	protected ManagedNeoServerLifecycleManager managedNeoServerLifecycleManager;

	private ManagedNeoServer() {
		super();
	}

	/**
	 * Builder to start neo4j server accordingly to your setup
	 */
	public static class Neo4jServerRuleBuilder {

		private ManagedNeoServerLifecycleManager managedNeoServerLifecycleManager;

		private Neo4jServerRuleBuilder() {
			this.managedNeoServerLifecycleManager = new ManagedNeoServerLifecycleManager();
		}

		public static Neo4jServerRuleBuilder newManagedNeo4jServerRule() {
			return new Neo4jServerRuleBuilder();
		}

		public Neo4jServerRuleBuilder neo4jPath(String neo4jPath) {
			this.managedNeoServerLifecycleManager.setNeo4jPath(neo4jPath);
			return this;
		}

		public Neo4jServerRuleBuilder targetPath(String targetPath) {
			this.managedNeoServerLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public Neo4jServerRuleBuilder port(int port) {
			this.managedNeoServerLifecycleManager.setPort(port);
			return this;
		}

		public ManagedNeoServer build() {
			if (this.managedNeoServerLifecycleManager.getNeo4jPath() == null) {
				throw new IllegalArgumentException("No Path to Neo4j is provided.");
			}
			
			ManagedNeoServer managedNeoServer = new ManagedNeoServer();
			managedNeoServer.managedNeoServerLifecycleManager = this.managedNeoServerLifecycleManager;
			
			return managedNeoServer;
		}

	}

	@Override
	protected void before() throws Throwable {
		this.managedNeoServerLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.managedNeoServerLifecycleManager.stopEngine();
	}

}
