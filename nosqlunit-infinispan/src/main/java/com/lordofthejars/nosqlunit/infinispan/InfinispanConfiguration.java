package com.lordofthejars.nosqlunit.infinispan;

import java.util.Properties;

import org.infinispan.commons.api.BasicCache;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public class InfinispanConfiguration extends AbstractJsr330Configuration {

	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 11222;
	
	private String cacheName;
	
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	
	private Properties configurationProperties = null;
	
	private BasicCache<Object, Object> cache;
	
	public InfinispanConfiguration() {
		super();
	}

	public String getCacheName() {
		return cacheName;
	}

	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public BasicCache<Object, Object> getCache() {
		return cache;
	}

	public void setCache(BasicCache<Object, Object> cache) {
		this.cache = cache;
	}
	
	public Properties getConfigurationProperties() {
		return configurationProperties;
	}
	
	public void setConfigurationProperties(Properties configurationProperties) {
		this.configurationProperties = configurationProperties;
	}
}
