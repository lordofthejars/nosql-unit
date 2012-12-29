package com.lordofthejars.nosqlunit.infinispan.integration;

import static com.lordofthejars.nosqlunit.infinispan.ManagedInfinispan.ManagedInfinispanRuleBuilder.newManagedInfinispanRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;

import org.infinispan.api.BasicCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.infinispan.InfinispanOperation;
import com.lordofthejars.nosqlunit.infinispan.ManagedInfinispan;
import com.lordofthejars.nosqlunit.infinispan.User;

public class WhenManagedInfinispanOperationsAreRequired {

	private static final String OBJECT_DATA = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"implementation\":\"com.lordofthejars.nosqlunit.infinispan.User\",\n" + 
			"				\"value\": {\n" + 
			"						\"username\":\"alex\"\n" + 
			"					 }			\n" + 
			"			},\n" +
			"			{\n" + 
			"				\"key\":\"key2\",\n" + 
			"				\"value\":3\n" + 
			"			}"+
			"		]\n" + 
			"}";
	
	@ClassRule
	public static ManagedInfinispan managedInfinispan = newManagedInfinispanRule().infinispanPath("/opt/infinispan-5.1.6").build(); 
	
	private RemoteCacheManager remoteCacheManager = new RemoteCacheManager();
	
	@After
	public void tearDown() {
		BasicCache<Object, Object> cache = remoteCacheManager.getCache();
		cache.clear();
	}
	
	
	@Test
	public void data_should_be_inserted_into_infinispan() {
		
		BasicCache<Object, Object> cache = remoteCacheManager.getCache();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(cache);
		infinispanOperation.insert(new ByteArrayInputStream(OBJECT_DATA.getBytes()));
		
		BasicCache<String, User> usedCache = infinispanOperation.connectionManager();
		assertThat(usedCache.get("key1"), is(new User("alex")));
	}
	
	@Test
	public void data_should_be_removed_from_infinispan() {
		
		BasicCache<Object, Object> cache = remoteCacheManager.getCache();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(cache);
		
		infinispanOperation.insert(new ByteArrayInputStream(OBJECT_DATA.getBytes()));
		infinispanOperation.deleteAll();
		
		BasicCache<String, String> usedCache = infinispanOperation.connectionManager();
		
		assertThat(usedCache.size(), is(0));
		
	}
	
	@Test
	public void data_should_be_compared_between_expected_and_current_data() {
		
		BasicCache<Object, Object> cache = remoteCacheManager.getCache();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(cache);
		infinispanOperation.insert(new ByteArrayInputStream(OBJECT_DATA.getBytes()));
		
		boolean sameDataset = infinispanOperation.databaseIs(new ByteArrayInputStream(OBJECT_DATA.getBytes()));
		assertThat(sameDataset, is(true));
		
	}
	
}
