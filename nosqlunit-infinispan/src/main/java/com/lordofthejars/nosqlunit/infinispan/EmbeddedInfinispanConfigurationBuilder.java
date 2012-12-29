package com.lordofthejars.nosqlunit.infinispan;

import org.infinispan.api.BasicCache;
import org.infinispan.manager.EmbeddedCacheManager;

import com.lordofthejars.nosqlunit.core.FailureHandler;


public class EmbeddedInfinispanConfigurationBuilder {

	private final InfinispanConfiguration infinispanConfiguration;
	
	private EmbeddedInfinispanConfigurationBuilder() {
		super();
		this.infinispanConfiguration = new InfinispanConfiguration();
	}
	
	public static EmbeddedInfinispanConfigurationBuilder newEmbeddedInfinispanConfiguration() {
		return new EmbeddedInfinispanConfigurationBuilder();
	}
	
	public EmbeddedInfinispanConfigurationBuilder connectionIdentifier(String connectionIdentifier) {
		this.infinispanConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}
	
	public EmbeddedInfinispanConfigurationBuilder cacheName(String cacheName) {
		this.infinispanConfiguration.setCacheName(cacheName);
		return this;
	}
	
	public InfinispanConfiguration build() {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		if(defaultEmbeddedInstance == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedInfinispan rule with default target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.");
		}
		
		BasicCache<Object, Object> basicCache = this.infinispanConfiguration.getCacheName() == null ? defaultEmbeddedInstance.getCache() : defaultEmbeddedInstance.getCache(this.infinispanConfiguration.getCacheName());  
		this.infinispanConfiguration.setCache(basicCache);
		
		return this.infinispanConfiguration;
	}
	
	public InfinispanConfiguration buildFromTargetPath(String targetPath) {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getEmbeddedByTargetPath(targetPath);
		
		if(defaultEmbeddedInstance == null) {
			throw FailureHandler.createIllegalStateFailure("There is no EmbeddedInfinispan rule with %s target defined during test execution. Please create one using @Rule or @ClassRule before executing these tests.", targetPath);
		}
		
		BasicCache<Object, Object> basicCache = this.infinispanConfiguration.getCacheName() == null ? defaultEmbeddedInstance.getCache() : defaultEmbeddedInstance.getCache(this.infinispanConfiguration.getCacheName());  
		this.infinispanConfiguration.setCache(basicCache);
		
		return this.infinispanConfiguration;
	}
	
}
