package com.lordofthejars.nosqlunit.neo4j;

import org.junit.rules.ExternalResource;

public class EmbeddedNeo4j extends ExternalResource {

	protected EmbeddedNeo4jLifecycleManager embeddedNeo4jLifecycleManager;

	private EmbeddedNeo4j() {
		super();
	}

	public static class EmbeddedNeo4jRuleBuilder {

		private EmbeddedNeo4jLifecycleManager embeddedNeo4jLifecycleManager;

		private EmbeddedNeo4jRuleBuilder() {
			this.embeddedNeo4jLifecycleManager = new EmbeddedNeo4jLifecycleManager();
		}

		public static EmbeddedNeo4jRuleBuilder newEmbeddedNeo4jRule() {
			return new EmbeddedNeo4jRuleBuilder();
		}

		public EmbeddedNeo4jRuleBuilder targetPath(String targetPath) {
			this.embeddedNeo4jLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public EmbeddedNeo4j build() {
			if (this.embeddedNeo4jLifecycleManager.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Neo4j is provided.");
			}
			
			EmbeddedNeo4j embeddedNeo4j = new EmbeddedNeo4j();
			embeddedNeo4j.embeddedNeo4jLifecycleManager = this.embeddedNeo4jLifecycleManager;
			
			return embeddedNeo4j;
		}

	}

	@Override
	protected void before() throws Throwable {
		this.embeddedNeo4jLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.embeddedNeo4jLifecycleManager.stopEngine();
	}
	
}
