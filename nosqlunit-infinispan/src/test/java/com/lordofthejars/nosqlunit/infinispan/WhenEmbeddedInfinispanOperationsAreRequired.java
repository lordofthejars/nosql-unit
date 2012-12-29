package com.lordofthejars.nosqlunit.infinispan;

import static com.lordofthejars.nosqlunit.infinispan.EmbeddedInfinispan.EmbeddedInfinispanRuleBuilder.newEmbeddedInfinispanRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;

import org.infinispan.Cache;
import org.infinispan.api.BasicCache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

public class WhenEmbeddedInfinispanOperationsAreRequired {

	private static final String SIMPLE_DATA = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"value\":\"alex\"			\n" + 
			"			}\n" + 
			"		]\n" + 
			"}\n" + 
			"";
	
	@ClassRule
	public static EmbeddedInfinispan embeddedInfinispan = newEmbeddedInfinispanRule().build(); 
	
	@After
	public void tearDown() {
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		BasicCache<Object, Object> cache = defaultEmbeddedInstance.getCache();
		cache.clear();
	}
	
	@Test
	public void data_should_be_inserted_into_infinispan() {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(defaultEmbeddedInstance.getCache());
		infinispanOperation.insert(new ByteArrayInputStream(SIMPLE_DATA.getBytes()));
		
		BasicCache<String, String> cache = infinispanOperation.connectionManager();
		assertThat(cache.get("key1"), is("alex"));
	}
	
	@Test
	public void data_should_be_removed_from_infinispan() {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(defaultEmbeddedInstance.getCache());
		
		infinispanOperation.insert(new ByteArrayInputStream(SIMPLE_DATA.getBytes()));
		infinispanOperation.deleteAll();
		
		BasicCache<String, String> cache = infinispanOperation.connectionManager();
		
		assertThat(cache.size(), is(0));
		
	}
	
	@Test
	public void data_should_be_compared_between_expected_and_current_data() {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(defaultEmbeddedInstance.getCache());
		infinispanOperation.insert(new ByteArrayInputStream(SIMPLE_DATA.getBytes()));
		
		boolean sameDataset = infinispanOperation.databaseIs(new ByteArrayInputStream(SIMPLE_DATA.getBytes()));
		assertThat(sameDataset, is(true));
		
	}
	
}
