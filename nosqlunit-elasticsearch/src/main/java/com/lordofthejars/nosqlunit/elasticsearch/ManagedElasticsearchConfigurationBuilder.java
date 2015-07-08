package com.lordofthejars.nosqlunit.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


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
