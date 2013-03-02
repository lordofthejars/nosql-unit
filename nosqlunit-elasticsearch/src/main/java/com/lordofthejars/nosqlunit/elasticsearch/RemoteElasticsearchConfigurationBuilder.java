package com.lordofthejars.nosqlunit.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

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
	
	public ElasticsearchConfiguration build() {
		
		TransportClient client = getClient();
		client.addTransportAddress(new InetSocketTransportAddress(this.elasticsearchConfiguration.getHost(), this.elasticsearchConfiguration.getPort()));
		this.elasticsearchConfiguration.setClient(client);
		
		return this.elasticsearchConfiguration;
	}
	
	private TransportClient getClient() {
		if(this.elasticsearchConfiguration.getSettings() == null) {
			return new TransportClient();
		} else {
			return new TransportClient(this.elasticsearchConfiguration.getSettings());
		}
	}

	
}
