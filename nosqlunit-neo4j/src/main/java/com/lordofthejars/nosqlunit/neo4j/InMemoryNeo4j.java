package com.lordofthejars.nosqlunit.neo4j;

import java.util.Map;

import org.junit.rules.ExternalResource;

public class InMemoryNeo4j extends ExternalResource {

	protected InMemoryNeo4jLifecycleManager inMemoryNeo4jLifecycleManager;

	private InMemoryNeo4j() {
		super();
	}

	public static class InMemoryNeo4jRuleBuilder {

		private InMemoryNeo4jLifecycleManager inMemoryNeo4jLifecycleManager;

		private InMemoryNeo4jRuleBuilder() {
			this.inMemoryNeo4jLifecycleManager = new InMemoryNeo4jLifecycleManager();
		}

		public static InMemoryNeo4jRuleBuilder newInMemoryNeo4j() {
			return new InMemoryNeo4jRuleBuilder();
		}

		public InMemoryNeo4jRuleBuilder configuration(Map<String, String> parameters) {
			this.inMemoryNeo4jLifecycleManager.getConfigurationParameters().putAll(parameters);
			return this;
		}

		public InMemoryNeo4j build() {
			
			InMemoryNeo4j inMemoryNeo4j = new InMemoryNeo4j();
			inMemoryNeo4j.inMemoryNeo4jLifecycleManager = this.inMemoryNeo4jLifecycleManager;
			
			return inMemoryNeo4j;
		}

	}

	@Override
	protected void before() throws Throwable {
		this.inMemoryNeo4jLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.inMemoryNeo4jLifecycleManager.stopEngine();
	}

}
