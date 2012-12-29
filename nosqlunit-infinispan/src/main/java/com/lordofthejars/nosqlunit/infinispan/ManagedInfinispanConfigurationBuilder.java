package com.lordofthejars.nosqlunit.infinispan;

import java.util.Properties;

import org.infinispan.api.BasicCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

public class ManagedInfinispanConfigurationBuilder {

	private final InfinispanConfiguration infinispanConfiguration;
	
	private ManagedInfinispanConfigurationBuilder() {
		super();
		this.infinispanConfiguration = new InfinispanConfiguration();
	}
	
	public static ManagedInfinispanConfigurationBuilder newManagedInfinispanConfiguration() {
		return new ManagedInfinispanConfigurationBuilder();
	}
	
	public ManagedInfinispanConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.infinispanConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public ManagedInfinispanConfigurationBuilder cacheName(String cacheName) {
		this.infinispanConfiguration.setCacheName(cacheName);
		return this;
	}
	
	public ManagedInfinispanConfigurationBuilder port(int port) {
		this.infinispanConfiguration.setPort(port);
		return this;
	}
	
	public ManagedInfinispanConfigurationBuilder configurationProperties(Properties configurationProperties) {
		this.infinispanConfiguration.setConfigurationProperties(configurationProperties);
		return this;
	}
	
	public InfinispanConfiguration build() {
		
		RemoteCacheManager remoteCacheManager = this.infinispanConfiguration.getConfigurationProperties() == null ? new RemoteCacheManager() : new RemoteCacheManager(this.infinispanConfiguration.getConfigurationProperties());
		
		BasicCache<Object, Object> basicCache = this.infinispanConfiguration.getCacheName() == null ? remoteCacheManager.getCache() : remoteCacheManager.getCache(this.infinispanConfiguration.getCacheName());  
		this.infinispanConfiguration.setCache(basicCache);
		
		return this.infinispanConfiguration;
	}
	
}
