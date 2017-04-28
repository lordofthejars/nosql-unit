package com.lordofthejars.nosqlunit.infinispan;

import java.util.Properties;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

public class RemoteInfinispanConfigurationBuilder {

private final InfinispanConfiguration infinispanConfiguration;
	
	private RemoteInfinispanConfigurationBuilder() {
		super();
		this.infinispanConfiguration = new InfinispanConfiguration();
	}
	
	public static RemoteInfinispanConfigurationBuilder newRemoteInfinispanConfiguration() {
		return new RemoteInfinispanConfigurationBuilder();
	}
	
	public RemoteInfinispanConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.infinispanConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public RemoteInfinispanConfigurationBuilder cacheName(String cacheName) {
		this.infinispanConfiguration.setCacheName(cacheName);
		return this;
	}
	
	public RemoteInfinispanConfigurationBuilder host(String host) {
		this.infinispanConfiguration.setHost(host);
		return this;
	}
	
	public RemoteInfinispanConfigurationBuilder port(int port) {
		this.infinispanConfiguration.setPort(port);
		return this;
	}
	
	public RemoteInfinispanConfigurationBuilder configurationProperties(Properties configurationProperties) {
		this.infinispanConfiguration.setConfigurationProperties(configurationProperties);
		return this;
	}
	
	public InfinispanConfiguration build() {
		ConfigurationBuilder builder = new ConfigurationBuilder();
		if (this.infinispanConfiguration.getConfigurationProperties() != null)
			builder.withProperties(this.infinispanConfiguration.getConfigurationProperties());

		RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());
		
		BasicCache<Object, Object> basicCache = this.infinispanConfiguration.getCacheName() == null ? remoteCacheManager.getCache() : remoteCacheManager.getCache(this.infinispanConfiguration.getCacheName());  
		this.infinispanConfiguration.setCache(basicCache);
		
		return this.infinispanConfiguration;
	}
	
}
