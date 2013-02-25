package com.lordofthejars.nosqlunit.elasticsearch;


import static com.lordofthejars.nosqlunit.elasticsearch.ManagedElasticsearchConfigurationBuilder.managedElasticsearch;
import static com.lordofthejars.nosqlunit.elasticsearch.EmbeddedElasticsearchConfigurationBuilder.embeddedElasticsearch;

import org.elasticsearch.client.Client;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class ElasticsearchRule extends AbstractNoSqlTestRule {

	private static final String EXTENSION = "json";
	
	private DatabaseOperation<? extends Client> databaseOperation;

	public static class ElasticsearchRuleBuilder {
		
		private ElasticsearchConfiguration elasticsearchConfiguration;
		private Object target;
		
		private ElasticsearchRuleBuilder() {
		}
		
		public static ElasticsearchRuleBuilder newElasticsearchRule() {
			return new ElasticsearchRuleBuilder();
		}
		
		public ElasticsearchRuleBuilder configure(ElasticsearchConfiguration elasticsearchConfiguration) {
			this.elasticsearchConfiguration = elasticsearchConfiguration;
			return this;
		}
		
		public ElasticsearchRuleBuilder unitInstance(Object target) {
			this.target = target;
			return this;
		}
		
		public ElasticsearchRule defaultEmbeddedElasticsearch() {
			return new ElasticsearchRule(embeddedElasticsearch().build());
		}
		
		public ElasticsearchRule defaultManagedElasticsearch() {
			return new ElasticsearchRule(managedElasticsearch().build());
		}
		
		public ElasticsearchRule build() {
			
			if(this.elasticsearchConfiguration == null) {
				throw new IllegalArgumentException("Configuration object should be provided.");
			}
			
			return new ElasticsearchRule(elasticsearchConfiguration, target);
		}
		
	}
	
	public ElasticsearchRule(ElasticsearchConfiguration elasticsearchConfiguration) {
		super(elasticsearchConfiguration.getConnectionIdentifier());
		this.databaseOperation = new ElasticsearchOperation(elasticsearchConfiguration.getClient());
	}
	
	/*With JUnit 10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is apporach is the only way.*/
	public ElasticsearchRule(ElasticsearchConfiguration elasticsearchConfiguration, Object target) {
		this(elasticsearchConfiguration);
		setTarget(target);
	}

	@Override
	public DatabaseOperation getDatabaseOperation() {
		return this.databaseOperation;
	}

	@Override
	public String getWorkingExtension() {
		return EXTENSION;
	}

}
