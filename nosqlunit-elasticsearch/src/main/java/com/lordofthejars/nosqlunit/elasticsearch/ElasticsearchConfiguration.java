package com.lordofthejars.nosqlunit.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public class ElasticsearchConfiguration extends AbstractJsr330Configuration {

	private static final String LOCALHOST = "localhost";
	private static final int DEFAULT_PORT = 9300;
	
	private String host = LOCALHOST;
	private int port = DEFAULT_PORT;
	private Settings settings = null;
	
	private Client client;
	
	public void setClient(Client client) {
		this.client = client;
	}
	
	public Client getClient() {
		return client;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setSettings(Settings settings) {
		this.settings = settings;
	}
	
	public Settings getSettings() {
		return settings;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public String getHost() {
		return host;
	}
	
}
