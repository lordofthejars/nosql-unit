package com.lordofthejars.nosqlunit.elasticsearch2;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;

public class RemoteElasticsearchConfigurationBuilder {

	private ElasticsearchConfiguration elasticsearchConfiguration = new ElasticsearchConfiguration();

	private RemoteElasticsearchConfigurationBuilder() {
		super();
	}

	public static RemoteElasticsearchConfigurationBuilder remoteElasticsearch() {
		return new RemoteElasticsearchConfigurationBuilder();
	}

	public RemoteElasticsearchConfigurationBuilder port(int port) {
		this.elasticsearchConfiguration.setPort(port);
		return this;
	}

	public RemoteElasticsearchConfigurationBuilder host(String host) {
		this.elasticsearchConfiguration.setHost(host);
		return this;
	}

	public RemoteElasticsearchConfigurationBuilder settings(Settings settings) {
		this.elasticsearchConfiguration.setSettings(settings);
		return this;
	}

	public RemoteElasticsearchConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
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
