package com.lordofthejars.nosqlunit.elasticsearch;

import org.elasticsearch.client.Client;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public class ElasticsearchConfiguration extends AbstractJsr330Configuration {

	private Client client;
	
	public void setClient(Client client) {
		this.client = client;
	}
	
	public Client getClient() {
		return client;
	}
	
}
