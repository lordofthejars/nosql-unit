package com.lordofthejars.nosqlunit.infinispan;

import static com.lordofthejars.nosqlunit.infinispan.EmbeddedInfinispanConfigurationBuilder.newEmbeddedInfinispanConfiguration;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.Test;

public class WhenEmbeddedConfigurationIsRequired {

	@Test
	public void in_memory_configuration_should_use_default_embedded_instance() {
		
		EmbeddedCacheManager embeddedInstance = mock(EmbeddedCacheManager.class);
		Cache cache = mock(	Cache.class);
		when(embeddedInstance.getCache()).thenReturn(cache);
		
		EmbeddedInfinispanInstancesFactory.getInstance().addEmbeddedInstance(embeddedInstance, "a");
		InfinispanConfiguration configuration = newEmbeddedInfinispanConfiguration().build();
		
		assertThat(cache, is((Cache)configuration.getCache()));
		
		EmbeddedInfinispanInstancesFactory.getInstance().removeEmbeddedInstance("a");
	}
	
	@Test(expected=IllegalStateException.class)
	public void in_memory_configuration_should_throw_an_exception_if_no_default_embedded() {
		InfinispanConfiguration configuration = newEmbeddedInfinispanConfiguration().build();
	}

	@Test
	public void in_memory_configuration_should_use_targeted_instance() {
		
		EmbeddedCacheManager embeddedInstanceA = mock(EmbeddedCacheManager.class);
		Cache cacheA = mock(	Cache.class);
		when(embeddedInstanceA.getCache()).thenReturn(cacheA);
		
		EmbeddedInfinispanInstancesFactory.getInstance().addEmbeddedInstance(embeddedInstanceA, "a");
		
		EmbeddedCacheManager embeddedInstanceB = mock(EmbeddedCacheManager.class);
		Cache cacheB = mock(	Cache.class);
		when(embeddedInstanceB.getCache()).thenReturn(cacheB);
		
		EmbeddedInfinispanInstancesFactory.getInstance().addEmbeddedInstance(embeddedInstanceB, "b");
		
		InfinispanConfiguration configuration = newEmbeddedInfinispanConfiguration().buildFromTargetPath("b");
		
		assertThat(cacheB, is((Cache)configuration.getCache()));
		
		EmbeddedInfinispanInstancesFactory.getInstance().removeEmbeddedInstance("a");
		EmbeddedInfinispanInstancesFactory.getInstance().removeEmbeddedInstance("b");
		
	}
	
	@Test(expected=IllegalStateException.class)
	public void in_memory_configuration_should_throw_an_exception_if_no_targeted_instance() {
		InfinispanConfiguration configuration = newEmbeddedInfinispanConfiguration().buildFromTargetPath("b");
	}
	
}
