package com.lordofthejars.nosqlunit.elasticsearch;

import org.junit.rules.ExternalResource;

public class ManagedElasticsearch extends ExternalResource {

	private ManagedElasticsearch() {
		super();
	}
	
	protected ManagedElasticsearchLifecycleManager managedElasticsearchLifecycleManager;
	

	public static class ManagedElasticsearchRuleBuilder {

		private ManagedElasticsearchLifecycleManager managedElasticsearchLifecycleManager;
		
		private ManagedElasticsearchRuleBuilder() {
			this.managedElasticsearchLifecycleManager = new ManagedElasticsearchLifecycleManager();
		}

		public static ManagedElasticsearchRuleBuilder newManagedElasticsearchRule() {
			return new ManagedElasticsearchRuleBuilder();
		}

		public ManagedElasticsearchRuleBuilder elasticsearchPath(String elasticsearchPath) {
			this.managedElasticsearchLifecycleManager.setElasticsearchPath(elasticsearchPath);
			return this;
		}

		public ManagedElasticsearchRuleBuilder port(int port) {
			this.managedElasticsearchLifecycleManager.setPort(port);
			return this;
		}

		
		public ManagedElasticsearchRuleBuilder targetPath(String targetPath) {
			this.managedElasticsearchLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		
		public ManagedElasticsearchRuleBuilder appendCommandLineArguments(
				String argumentName, String argumentValue) {
			this.managedElasticsearchLifecycleManager.addExtraCommandLineArgument(argumentName,
					argumentValue);
			return this;
		}

		public ManagedElasticsearchRuleBuilder appendSingleCommandLineArguments(
				String argument) {
			this.managedElasticsearchLifecycleManager.addSingleCommandLineArgument(argument);
			return this;
		}

		
		public ManagedElasticsearch build() {
			if (this.managedElasticsearchLifecycleManager.getElasticsearchPath() == null) {
				throw new IllegalArgumentException(
						"No Path to Elasticsearch is provided.");
			}
			
			ManagedElasticsearch managedElasticsearch = new ManagedElasticsearch();
			managedElasticsearch.managedElasticsearchLifecycleManager = this.managedElasticsearchLifecycleManager;
			
			return managedElasticsearch;
		}
	}
	
	@Override
	protected void before() throws Throwable {
		this.managedElasticsearchLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.managedElasticsearchLifecycleManager.stopEngine();
	}

}
