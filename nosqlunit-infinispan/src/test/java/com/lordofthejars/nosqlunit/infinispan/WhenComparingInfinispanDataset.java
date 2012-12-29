package com.lordofthejars.nosqlunit.infinispan;

import static com.lordofthejars.nosqlunit.infinispan.EmbeddedInfinispan.EmbeddedInfinispanRuleBuilder.newEmbeddedInfinispanRule;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.is;

import java.io.ByteArrayInputStream;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class WhenComparingInfinispanDataset {

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
	
	private static final String OBJECT_DATA_DIFFERENT_NUMBER_KEYS = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"implementation\":\"com.lordofthejars.nosqlunit.infinispan.User\",\n" + 
			"				\"value\": {\n" + 
			"						\"username\":\"alex\"\n" + 
			"					 }			\n" + 
			"			}\n"+
			"		]\n" + 
			"}";
	
	private static final String OBJECT_DATA_DIFFERENT_VALUE = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"implementation\":\"com.lordofthejars.nosqlunit.infinispan.User\",\n" + 
			"				\"value\": {\n" + 
			"						\"username\":\"soto\"\n" + 
			"					 }			\n" + 
			"			}\n"+
			"		]\n" + 
			"}";
	
	private static final String OBJECT_DATA_DIFFERENT_KEYS = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key2\",\n" + 
			"				\"implementation\":\"com.lordofthejars.nosqlunit.infinispan.User\",\n" + 
			"				\"value\": {\n" + 
			"						\"username\":\"alex\"\n" + 
			"					 }			\n" + 
			"			}\n"+
			"		]\n" + 
			"}";
	
	@ClassRule
	public static EmbeddedInfinispan embeddedInfinispan = newEmbeddedInfinispanRule().build(); 
	
	@After
	public void tearDown() {
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		Cache<Object, Object> cache = defaultEmbeddedInstance.getCache();
		cache.clear();
	}
	
	@Test
	public void no_exception_should_be_thrown_if_data_is_the_expected() {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(defaultEmbeddedInstance.getCache());
		infinispanOperation.insert(new ByteArrayInputStream(OBJECT_DATA.getBytes()));
		
		assertThat(infinispanOperation.databaseIs(new ByteArrayInputStream(OBJECT_DATA.getBytes())), is(true));
		
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_keys_are_different() {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(defaultEmbeddedInstance.getCache());
		infinispanOperation.insert(new ByteArrayInputStream(OBJECT_DATA.getBytes()));
		
		try {
			infinispanOperation.databaseIs(new ByteArrayInputStream(OBJECT_DATA_DIFFERENT_NUMBER_KEYS.getBytes()));
			fail();
		} catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Number of expected keys are 1 but was found 2."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_key_is_not_found() {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(defaultEmbeddedInstance.getCache());
		infinispanOperation.insert(new ByteArrayInputStream(OBJECT_DATA_DIFFERENT_NUMBER_KEYS.getBytes()));
		
		try {
			infinispanOperation.databaseIs(new ByteArrayInputStream(OBJECT_DATA_DIFFERENT_KEYS.getBytes()));
			fail();
		} catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key key2 was not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_element_is_different() {
		
		EmbeddedCacheManager defaultEmbeddedInstance = EmbeddedInfinispanInstancesFactory.getInstance().getDefaultEmbeddedInstance();
		
		InfinispanOperation infinispanOperation = new InfinispanOperation(defaultEmbeddedInstance.getCache());
		infinispanOperation.insert(new ByteArrayInputStream(OBJECT_DATA_DIFFERENT_NUMBER_KEYS.getBytes()));
		
		try {
			infinispanOperation.databaseIs(new ByteArrayInputStream(OBJECT_DATA_DIFFERENT_VALUE.getBytes()));
			fail();
		} catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Object for key key1 should be User [username=soto] but was found User [username=alex]."));
		}
		
	}
}
