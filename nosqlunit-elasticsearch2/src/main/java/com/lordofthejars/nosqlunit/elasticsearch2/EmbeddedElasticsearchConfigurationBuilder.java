package com.lordofthejars.nosqlunit.elasticsearch2;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import org.elasticsearch.node.Node;

public class EmbeddedElasticsearchConfigurationBuilder {

	private ElasticsearchConfiguration elasticsearchConfiguration = new ElasticsearchConfiguration();

	public static EmbeddedElasticsearchConfigurationBuilder embeddedElasticsearch() {
		return new EmbeddedElasticsearchConfigurationBuilder();
	}

	public EmbeddedElasticsearchConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		elasticsearchConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}

	public ElasticsearchConfiguration build() {

		Node defaultEmbeddedInstance = EmbeddedElasticsearchInstancesFactory.getInstance().getDefaultEmbeddedInstance();

		if (defaultEmbeddedInstance == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedElasticsearch rule with default target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
		}

		this.elasticsearchConfiguration.setClient(defaultEmbeddedInstance.client());
		return this.elasticsearchConfiguration;
	}
}
