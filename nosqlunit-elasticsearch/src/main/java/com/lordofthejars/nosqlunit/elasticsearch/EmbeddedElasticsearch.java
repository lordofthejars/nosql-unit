package com.lordofthejars.nosqlunit.elasticsearch;

import org.elasticsearch.common.settings.Settings;
import org.junit.rules.ExternalResource;

public class EmbeddedElasticsearch extends ExternalResource {

	private EmbeddedElasticsearch() {
		super();
	}
	
	protected EmbeddedElasticsearchLifecycleManager elasticsearchLifecycleManager;
	
	public static class EmbeddedElasticsearchRuleBuilder {
		
		private EmbeddedElasticsearchLifecycleManager elasticsearchLifecycleManager;
		
		private EmbeddedElasticsearchRuleBuilder() {
			this.elasticsearchLifecycleManager = new EmbeddedElasticsearchLifecycleManager();
		}
		
		public static EmbeddedElasticsearchRuleBuilder newEmbeddedElasticsearchRule() {
			return new EmbeddedElasticsearchRuleBuilder();
		}
		
		public EmbeddedElasticsearchRuleBuilder targetPath(String targetPath) {
			this.elasticsearchLifecycleManager.setTargetPath(targetPath);
			return this;
		}

		public EmbeddedElasticsearchRuleBuilder clusterName(String clusterName) {
			this.elasticsearchLifecycleManager.setClusterName(clusterName);
			return this;
		}
		
		public EmbeddedElasticsearchRuleBuilder client(boolean client) {
			this.elasticsearchLifecycleManager.setClient(client);
			return this;
		}
		
		public EmbeddedElasticsearchRuleBuilder settings(Settings settings) {
			this.elasticsearchLifecycleManager.setSettings(settings);
			return this;
		}
		
		public EmbeddedElasticsearchRuleBuilder local(boolean local) {
			this.elasticsearchLifecycleManager.setLocal(local);
			return this;
		}
		
		public EmbeddedElasticsearchRuleBuilder data(boolean data) {
			this.elasticsearchLifecycleManager.setData(data);
			return this;
		}
		
		public EmbeddedElasticsearchRuleBuilder loadConfigSettings(boolean loadConfigSettings) {
			this.elasticsearchLifecycleManager.setLoadConfigSettings(loadConfigSettings);
			return this;
		}
		
		public EmbeddedElasticsearch build() {
			
			if(this.elasticsearchLifecycleManager.getTargetPath() == null) {
				throw new IllegalArgumentException("No Path to Embedded Elasticsearch is provided.");
			}
			
			EmbeddedElasticsearch embeddedElasticsearch = new EmbeddedElasticsearch();
			embeddedElasticsearch.elasticsearchLifecycleManager = this.elasticsearchLifecycleManager;
			
			return embeddedElasticsearch;
			
		}
		
	}

	@Override
	protected void before() throws Throwable {
		this.elasticsearchLifecycleManager.startEngine();
	}

	@Override
	protected void after() {
		this.elasticsearchLifecycleManager.stopEngine();
	}
	
	
	
}
