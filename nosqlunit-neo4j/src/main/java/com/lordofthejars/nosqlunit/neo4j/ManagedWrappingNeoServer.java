package com.lordofthejars.nosqlunit.neo4j;

import org.junit.rules.ExternalResource;

public class ManagedWrappingNeoServer extends ExternalResource {

	protected ManagedWrappingNeoServerLifecycleManager managedWrappingNeoServerLifecycleManager;

	private ManagedWrappingNeoServer() {
		super();
	}
	
	public static class ManagedWrappingNeoServerRuleBuilder {

		private ManagedWrappingNeoServerLifecycleManager managedWrappingNeoServerLifecycleManager;

		private ManagedWrappingNeoServerRuleBuilder() {
			this.managedWrappingNeoServerLifecycleManager = new ManagedWrappingNeoServerLifecycleManager();
		}

		public static ManagedWrappingNeoServerRuleBuilder newWrappingNeoServerNeo4jRule() {
			return new ManagedWrappingNeoServerRuleBuilder();
		}

		public ManagedWrappingNeoServerRuleBuilder port(int port) {
			this.managedWrappingNeoServerLifecycleManager.setPort(port);
			return this;
		}
		
		public ManagedWrappingNeoServerRuleBuilder targetPath(String targetPath) {
			this.managedWrappingNeoServerLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public ManagedWrappingNeoServer build() {
			if (this.managedWrappingNeoServerLifecycleManager.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Neo4j is provided.");
			}
			
			ManagedWrappingNeoServer managedWrappingNeoServer = new ManagedWrappingNeoServer();
			managedWrappingNeoServer.managedWrappingNeoServerLifecycleManager = this.managedWrappingNeoServerLifecycleManager;
			
			return managedWrappingNeoServer;
		}

	}

	@Override
	protected void before() throws Throwable {
		this.managedWrappingNeoServerLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.managedWrappingNeoServerLifecycleManager.stopEngine();
	}
	
}
