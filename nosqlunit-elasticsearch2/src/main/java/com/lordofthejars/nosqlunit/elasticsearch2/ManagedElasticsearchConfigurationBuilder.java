package com.lordofthejars.nosqlunit.elasticsearch2;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;


public class ManagedElasticsearchConfigurationBuilder {
	private ElasticsearchConfiguration elasticsearchConfiguration = new ElasticsearchConfiguration();

	private ManagedElasticsearchConfigurationBuilder() {
		super();
	}

	public static ManagedElasticsearchConfigurationBuilder managedElasticsearch() {
		return new ManagedElasticsearchConfigurationBuilder();
	}

	public ManagedElasticsearchConfigurationBuilder port(int port) {
		this.elasticsearchConfiguration.setPort(port);
		return this;
	}

	public ManagedElasticsearchConfigurationBuilder settings(Settings settings) {
		this.elasticsearchConfiguration.setSettings(settings);
		return this;
	}

	public ManagedElasticsearchConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.elasticsearchConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}

	public ElasticsearchConfiguration build() {
		final InetSocketAddress address = new InetSocketAddress(this.elasticsearchConfiguration.getHost(), this.elasticsearchConfiguration.getPort());
		final TransportClient client = getClient().addTransportAddress(new InetSocketTransportAddress(address));
		this.elasticsearchConfiguration.setClient(client);

		return this.elasticsearchConfiguration;
	}

	private TransportClient getClient() {
		if (this.elasticsearchConfiguration.getSettings() == null) {
			return TransportClient.builder().build();
		} else {
			return TransportClient.builder().settings(this.elasticsearchConfiguration.getSettings()).build();
		}
	}
}
