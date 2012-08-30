package com.lordofthejars.nosqlunit.redis.integration;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static com.lordofthejars.nosqlunit.redis.ManagedRedis.ManagedRedisRuleBuilder.newManagedRedisRule;

import java.io.ByteArrayInputStream;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.redis.ManagedRedis;
import com.lordofthejars.nosqlunit.redis.RedisOperation;

public class WhenComparingRedisDataset {

	
	private static final String INSERT_SIMPLE_DATA ="{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"				{\n" + 
			"					\"key\":\"key1\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				}\n" + 
			"			]\n" + 
			"			}\n" +
			"]\n" + 
			"}";
	
	private static final String INSERT_SIMPLE_DATA_WITH_TWO_ELEMENTS ="{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"				{\n" + 
			"					\"key\":\"key1\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				},\n" + 
			"				{\n" + 
			"					\"key\":\"key2\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				}\n" + 
			"			]\n" + 
			"			}\n" +
			"]\n" + 
			"}";
	
	private static final String INSERT_SIMPLE_DATA_WITH_DIFFERENT_KEY ="{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"				{\n" + 
			"					\"key\":\"key2\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				}\n" + 
			"			]\n" + 
			"			}\n" +
			"]\n" + 
			"}";
	
	private static final String INSERT_SIMPLE_DATA_WITH_DIFFERENT_VALUE ="{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"				{\n" + 
			"					\"key\":\"key1\", \n" + 
			"					\"value\":\"value2\"\n" + 
			"				}\n" + 
			"			]\n" + 
			"			}\n" +
			"]\n" + 
			"}";
	
	private static final String INSERT_SET_DATA ="{\n" + 
			"\"data\":[\n" + 
			"			{\"set\": [" +
			"							{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"},\n" + 
			"                  					{\"value\":\"value4\"}\n" + 
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_SET_DATA_WITH_REPEAT_ELEMENTS ="{\n" + 
			"\"data\":[\n" + 
			"			{\"set\": [" +
			"							{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"},\n" + 
			"									{\"value\":\"value3\"},\n" + 
			"                  					{\"value\":\"value4\"}\n" + 
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_SET_DATA_WITH_DIFFERENT_KEY ="{\n" + 
			"\"data\":[\n" + 
			"			{\"set\": [" +
			"							{\n" + 
			"              				\"key\":\"key2\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"},\n" + 
			"                  					{\"value\":\"value4\"}\n" + 
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_SET_DATA_WITH_DIFFERENT_VALUE ="{\n" + 
			"\"data\":[\n" + 
			"			{\"set\": [" +
			"							{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"},\n" + 
			"                  					{\"value\":\"value5\"}\n" + 
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_SET_DATA_WITH_DIFFERENT_NUMBER_OF_ELEMENTS ="{\n" + 
			"\"data\":[\n" + 
			"			{\"set\": [" +
			"							{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"}\n"+
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_LIST_DATA ="{\n" + 
			"\"data\":[\n" + 
			"			{\"list\": [" +
			"							{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"},\n" + 
			"                  					{\"value\":\"value4\"}\n" + 
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_LIST_DATA_WITH_DIFFERENT_KEY ="{\n" + 
			"\"data\":[\n" + 
			"			{\"list\": [" +
			"							{\n" + 
			"              				\"key\":\"key2\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"},\n" + 
			"                  					{\"value\":\"value4\"}\n" + 
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_LIST_DATA_WITH_DIFFERENT_VALUE ="{\n" + 
			"\"data\":[\n" + 
			"			{\"list\": [" +
			"							{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"},\n" + 
			"                  					{\"value\":\"value5\"}\n" + 
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_LIST_DATA_WITH_DIFFERENT_NUMBER_OF_ELEMENTS ="{\n" + 
			"\"data\":[\n" + 
			"			{\"list\": [" +
			"							{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value3\"}\n"+
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_LIST_DATA_DIFFERENT_ORDER ="{\n" + 
			"\"data\":[\n" + 
			"			{\"list\": [" +
			"							{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              					\"values\":[\n" + 
			"                  					{\"value\":\"value4\"},\n" + 
			"                  					{\"value\":\"value3\"}\n" + 
			"              					]\n" + 
			"					 		}" +
			"					   ]\n" + 
			"      		}"+
			"]\n" + 
			"}";
	
	private static final String INSERT_SORT_SET ="{\n" + 
			"\"data\":[\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key1\",\n" + 
			"                     \"values\":[\n" + 
			"                           {\"score\":2, \"value\":\"value5\" },{\"score\":3, \"value\":\"1\" }, {\"score\":1, \"value\":\"value6\" }]\n" + 
			"                 }]\n" + 
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String INSERT_SORT_SET_WITH_ONE_ELEMENT ="{\n" + 
			"\"data\":[\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key1\",\n" + 
			"                     \"values\":[\n" + 
			"                           {\"score\":2, \"value\":\"value5\" }]\n" + 
			"                 }]\n" + 
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String INSERT_SORT_SET_DIFFERENT_ORDER ="{\n" + 
			"\"data\":[\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key1\",\n" + 
			"                     \"values\":[\n" + 
			"                           {\"score\":1, \"value\":\"value5\" },{\"score\":2, \"value\":\"1\" }, {\"score\":3, \"value\":\"value6\" }]\n" + 
			"                 }]\n" + 
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String INSERT_SORT_SET_DIFFERENT_KEY ="{\n" + 
			"\"data\":[\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key2\",\n" + 
			"                     \"values\":[\n" + 
			"                           {\"score\":2, \"value\":\"value5\" },{\"score\":3, \"value\":\"1\" }, {\"score\":1, \"value\":\"value6\" }]\n" + 
			"                 }]\n" + 
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String INSERT_SORT_SET_ORDER ="{\n" + 
			"\"data\":[\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key1\",\n" + 
			"                     \"values\":[\n" + 
			"                           {\"score\":1, \"value\":\"value6\" },{\"score\":2, \"value\":\"value5\" }, {\"score\":3, \"value\":\"1\" }]\n" + 
			"                 }]\n" + 
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String INSERT_SORT_SET_ORDER_DIFFERENT_VALUE ="{\n" + 
			"\"data\":[\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key1\",\n" + 
			"                     \"values\":[\n" + 
			"                           {\"score\":1, \"value\":\"value6\" },{\"score\":2, \"value\":\"value5\" }, {\"score\":3, \"value\":\"2\" }]\n" + 
			"                 }]\n" + 
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String DATA_WITH_HASH_TYPES = "{\n" + 
			"\"data\":[\n" + 
			"      		{\"hash\": [\n" + 
			"      					{\n" + 
			"      						\"key\":\"user\",\n" + 
			"      						\"values\":[\n" + 
			"      							{\"field\":\"name\", \"value\":\"alex\"},\n" + 
			"      							{\"field\":\"password\", \"value\":\"alex\"}\n" + 
			"      						]\n" + 
			"      					}\n" + 
			"      				]\n" + 
			"      		}\n" + 
			"		]\n" + 
			"}";
	
	private static final String DATA_WITH_HASH_TYPES_WITH_DIFFERENT_FIELD_VALUE = "{\n" + 
			"\"data\":[\n" + 
			"      		{\"hash\": [\n" + 
			"      					{\n" + 
			"      						\"key\":\"user\",\n" + 
			"      						\"values\":[\n" + 
			"      							{\"field\":\"name\", \"value\":\"alex\"},\n" + 
			"      							{\"field\":\"password\", \"value\":\"alex2\"}\n" + 
			"      						]\n" + 
			"      					}\n" + 
			"      				]\n" + 
			"      		}\n" + 
			"		]\n" + 
			"}";
	
	private static final String DATA_WITH_HASH_TYPES_WITH_DIFFERENT_FIELDS = "{\n" + 
			"\"data\":[\n" + 
			"      		{\"hash\": [\n" + 
			"      					{\n" + 
			"      						\"key\":\"user\",\n" + 
			"      						\"values\":[\n" + 
			"      							{\"field\":\"name\", \"value\":\"alex\"},\n" + 
			"      							{\"field\":\"address\", \"value\":\"alex\"}\n" + 
			"      						]\n" + 
			"      					}\n" + 
			"      				]\n" + 
			"      		}\n" + 
			"		]\n" + 
			"}";
	
	private static final String DATA_WITH_HASH_TYPES_WITH_DIFFERENT_KEYS = "{\n" + 
			"\"data\":[\n" + 
			"      		{\"hash\": [\n" + 
			"      					{\n" + 
			"      						\"key\":\"user2\",\n" + 
			"      						\"values\":[\n" + 
			"      							{\"field\":\"name\", \"value\":\"alex\"},\n" + 
			"      							{\"field\":\"password\", \"value\":\"alex\"}\n" + 
			"      						]\n" + 
			"      					}\n" + 
			"      				]\n" + 
			"      		}\n" + 
			"		]\n" + 
			"}";
	
	private static final String DATA_WITH_HASH_TYPES_WITH_DIFFERENT_NUMBER_OF_FIELDS = "{\n" + 
			"\"data\":[\n" + 
			"      		{\"hash\": [\n" + 
			"      					{\n" + 
			"      						\"key\":\"user\",\n" + 
			"      						\"values\":[\n" + 
			"      							{\"field\":\"password\", \"value\":\"alex\"}\n" + 
			"      						]\n" + 
			"      					}\n" + 
			"      				]\n" + 
			"      		}\n" + 
			"		]\n" + 
			"}";
	
	private static final String INSERT_SIMPLE_DATA_WITH_HASH_KEY ="{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"				{\n" + 
			"					\"key\":\"user\", \n" + 
			"					\"value\":\"value1\"\n" + 
			"				}\n" + 
			"			]\n" + 
			"			}\n" +
			"]\n" + 
			"}";
	
	@ClassRule
	public static ManagedRedis managedRedis = newManagedRedisRule().redisPath("/opt/redis-2.4.16").build();
	
	@After
	public void tearDown() {
		Jedis jedis = new Jedis("localhost", 6379);
		jedis.flushAll();
	}
	
	@Test
	public void no_exception_should_be_thrown_if_simple_content_is_expected() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SIMPLE_DATA.getBytes()));
		
		boolean isExpectedData = redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SIMPLE_DATA.getBytes()));
		assertThat(isExpectedData, is(true));
		
	}
	
	@Test
	public void exception_should_be_thrown_if_type_is_not_expected_with_simple_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SIMPLE_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_LIST_DATA.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element with key key1 is not a list."));
		}
		
	}

	@Test
	public void exception_should_be_thrown_if_key_is_not_expected_with_simple_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SIMPLE_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SIMPLE_DATA_WITH_DIFFERENT_KEY.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key key2 is not found."));
		}
	}

	@Test
	public void exception_should_be_thrown_if_value_is_not_expected_with_simple_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SIMPLE_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SIMPLE_DATA_WITH_DIFFERENT_VALUE.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key key1 does not contain element value2 but value1."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_keys_are_not_equal() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SIMPLE_DATA_WITH_TWO_ELEMENTS.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SIMPLE_DATA.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Number of expected keys are 1 but was found 2."));
		}
		
	}
	
	@Test
	public void no_exception_should_be_thrown_if_set_contains_same_elements() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SET_DATA.getBytes()));
		
		boolean isExpectedData = redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SET_DATA.getBytes()));
		assertThat(isExpectedData, is(true));
		
	}
	
	@Test
	public void no_exception_should_be_thrown_if_set_contains_none_unique_elements() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SET_DATA.getBytes()));
		
		boolean isExpectedData = redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SET_DATA_WITH_REPEAT_ELEMENTS.getBytes()));
		assertThat(isExpectedData, is(true));
		
	}
	
	@Test
	public void exception_should_be_thrown_if_type_is_not_expected_with_set_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SET_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SIMPLE_DATA.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element with key key1 is not a string."));
		}
		
	}
	
	@Test
	public void exception_should_be_thrown_if_key_is_not_expected_with_set_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SET_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SET_DATA_WITH_DIFFERENT_KEY.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key key2 is not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_value_is_not_expected_with_set_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SET_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SET_DATA_WITH_DIFFERENT_VALUE.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element value4 is not found in set of key key1."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_values_are_not_expected_in_set() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SET_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SET_DATA_WITH_DIFFERENT_NUMBER_OF_ELEMENTS.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected number of elements for key key1 is 1 but was counted 2."));
		}
	}
	
	
	@Test
	public void no_exception_should_be_thrown_if_list_contains_same_elements_in_different_order() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_LIST_DATA.getBytes()));
		
		boolean isExpectedData = redisOperation.databaseIs(new ByteArrayInputStream(INSERT_LIST_DATA_DIFFERENT_ORDER.getBytes()));
		assertThat(isExpectedData, is(true));
		
	}
	
	@Test
	public void exception_should_be_thrown_if_type_is_not_expected_with_list_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_LIST_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SIMPLE_DATA.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element with key key1 is not a string."));
		}
		
	}
	
	@Test
	public void exception_should_be_thrown_if_key_is_not_expected_with_list_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_LIST_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_LIST_DATA_WITH_DIFFERENT_KEY.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key key2 is not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_value_is_not_expected_with_list_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_LIST_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_LIST_DATA_WITH_DIFFERENT_VALUE.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element value4 is not found in list of key key1."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_values_are_not_expected_in_list() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_LIST_DATA.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_LIST_DATA_WITH_DIFFERENT_NUMBER_OF_ELEMENTS.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected number of elements for key key1 is 1 but was counted 2."));
		}
	}
	
	@Test
	public void no_exception_should_be_thrown_if_sortset_contains_same_elements_in_order() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SORT_SET.getBytes()));
		
		boolean isExpectedData = redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SORT_SET_ORDER.getBytes()));
		assertThat(isExpectedData, is(true));
		
	}
	
	@Test
	public void exception_should_be_thrown_if_type_is_not_expected_with_sortset_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SORT_SET.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_LIST_DATA.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element with key key1 is not a list."));
		}
		
	}
	
	@Test
	public void exception_should_be_thrown_if_key_is_not_expected_with_sortset_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SORT_SET.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SORT_SET_DIFFERENT_KEY.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key key2 is not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_value_is_not_expected_with_sortset_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SORT_SET.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SORT_SET_ORDER_DIFFERENT_VALUE.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element 1 is not found in set with same order of key key1."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_value_order_is_not_expected_in_sortset_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SORT_SET.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SORT_SET_DIFFERENT_ORDER.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element value6 is not found in set with same order of key key1."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_values_are_not_expected_in_sortset() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(INSERT_SORT_SET.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SORT_SET_WITH_ONE_ELEMENT.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected number of elements for key key1 is 1 but was counted 3."));
		}
	}
	
	@Test
	public void no_exception_should_be_thrown_if_hash_contains_same_fields() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(DATA_WITH_HASH_TYPES.getBytes()));
		
		boolean isExpectedData = redisOperation.databaseIs(new ByteArrayInputStream(DATA_WITH_HASH_TYPES.getBytes()));
		assertThat(isExpectedData, is(true));
		
	}
	
	@Test
	public void exception_should_be_thrown_if_type_is_not_expected_with_hash_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(DATA_WITH_HASH_TYPES.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(INSERT_SIMPLE_DATA_WITH_HASH_KEY.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Element with key user is not a string."));
		}
		
	}
	
	@Test
	public void exception_should_be_thrown_if_key_is_not_expected_with_hash_type() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(DATA_WITH_HASH_TYPES.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(DATA_WITH_HASH_TYPES_WITH_DIFFERENT_KEYS.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key user2 is not found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_number_of_fields_are_not_expected_in_hash() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(DATA_WITH_HASH_TYPES.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(DATA_WITH_HASH_TYPES_WITH_DIFFERENT_NUMBER_OF_FIELDS.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Expected fields for key user are 1 but 2 was found."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_field_is_not_present_in_hash() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(DATA_WITH_HASH_TYPES.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(DATA_WITH_HASH_TYPES_WITH_DIFFERENT_FIELDS.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Field address is not found for key user."));
		}
	}
	
	@Test
	public void exception_should_be_thrown_if_field_value_is_not_present_in_hash() {
		
		Jedis jedis = new Jedis("localhost", 6379);
		RedisOperation redisOperation = new RedisOperation(jedis);
		
		redisOperation.insert(new ByteArrayInputStream(DATA_WITH_HASH_TYPES.getBytes()));
		
		try {
			redisOperation.databaseIs(new ByteArrayInputStream(DATA_WITH_HASH_TYPES_WITH_DIFFERENT_FIELD_VALUE.getBytes()));
			fail();
		}catch(NoSqlAssertionError e) {
			assertThat(e.getMessage(), is("Key user and field password does not contain element alex2 but alex."));
		}
	}
	
}
