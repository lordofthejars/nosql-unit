package com.lordofthejars.nosqlunit.infinispan;

import org.infinispan.manager.EmbeddedCacheManager;

import com.lordofthejars.nosqlunit.util.EmbeddedInstances;

public class EmbeddedInfinispanInstancesFactory {

	private static EmbeddedInstances<EmbeddedCacheManager> embeddedInstances;
	
	private EmbeddedInfinispanInstancesFactory() {
		super();
	}
	
	public synchronized static EmbeddedInstances<EmbeddedCacheManager> getInstance() {
		if(embeddedInstances == null) {
			embeddedInstances = new EmbeddedInstances<EmbeddedCacheManager>();
		}
		
		return embeddedInstances;
	}
	
}
