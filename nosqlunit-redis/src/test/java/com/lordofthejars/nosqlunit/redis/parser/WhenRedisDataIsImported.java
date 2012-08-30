package com.lordofthejars.nosqlunit.redis.parser;

import static org.hamcrest.collection.IsMapContaining.hasValue;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.Jedis;

public class WhenRedisDataIsImported {

	@Mock
	private Jedis jedis;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	private static final String DATA_WITH_SIMPLE_TYPES = "{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"					{\n" + 
			"						\"key\":\"key1\", \n" + 
			"						\"value\":\"value\"\n" + 
			"					},\n" + 
			"      				{\n" + 
			"      					\"key\":\"key2\", \n" + 
			"      					\"value\":1.1\n" + 
			"      				}\n" + 
			"				]\n" + 
			"			}\n" + 
			"	]\n" + 
			"}";
	
	private static final String DATA_WITH_SIMPLE_TYPES_AND_TTL = "{\n" + 
			"\"data\":[\n" + 
			"			{\"simple\": [\n" + 
			"					{\n" + 
			"						\"key\":\"key1\", \n" + 
			"						\"value\":\"value\"\n" + 
			"						\"expireSeconds\":10\n" +				
			"					},\n" + 
			"      				{\n" + 
			"      					\"key\":\"key2\", \n" + 
			"      					\"value\":1.1\n" + 
			"      				}\n" + 
			"				]\n" + 
			"			}\n" + 
			"	]\n" + 
			"}";
	
	private static final String DATA_WITH_LIST_TYPES = "{\n" + 
			"\"data\":[\n" + 
			"			\n" + 
			"      		{\"list\": [" +
			"						{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              				\"values\":[\n" + 
			"                  				{\"value\":\"value1\"},\n" + 
			"                  				{\"value\":\"value2\"}\n" + 
			"              				]\n" + 
			"						},\n" + 
			"      					{\n" + 
			"              				\"key\":\"key2\",\n" + 
			"              				\"values\":[\n" + 
			"                  				{\"value\":\"value3\"},\n" + 
			"                  				{\"value\":\"value4\"}\n" + 
			"              				]\n" + 
			"						}\n" +
			"					   ]\n" +						
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String DATA_WITH_LIST_TYPES_WITH_TTL = "{\n" + 
			"\"data\":[\n" + 
			"			\n" + 
			"      		{\"list\": [" +
			"						{\n" + 
			"              				\"key\":\"key1\",\n" +
			"							\"expireSeconds\":10\n" +
			"              				\"values\":[\n" + 
			"                  				{\"value\":\"value1\"},\n" + 
			"                  				{\"value\":\"value2\"}\n" + 
			"              				]\n" + 
			"						},\n" + 
			"      					{\n" + 
			"              				\"key\":\"key2\",\n" + 
			"              				\"values\":[\n" + 
			"                  				{\"value\":\"value3\"},\n" + 
			"                  				{\"value\":\"value4\"}\n" + 
			"              				]\n" + 
			"						}\n" +
			"					   ]\n" +						
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String DATA_WITH_SET_TYPES = "{\n" + 
			"\"data\":[\n" + 
			"			\n" + 
			"      		{\"set\": [" +
			"						{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"              				\"values\":[\n" + 
			"                  				{\"value\":\"value1\"},\n" + 
			"                  				{\"value\":\"value2\"}\n" + 
			"              				]\n" + 
			"						},\n" + 
			"      					{\n" + 
			"              				\"key\":\"key2\",\n" + 
			"              				\"values\":[\n" + 
			"                  				{\"value\":\"value3\"},\n" + 
			"                  				{\"value\":\"value4\"}\n" + 
			"              				]\n" + 
			"						}\n" +
			"					   ]\n" +						
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String DATA_WITH_SET_TYPES_WITH_TTL = "{\n" + 
			"\"data\":[\n" + 
			"			\n" + 
			"      		{\"set\": [" +
			"						{\n" + 
			"              				\"key\":\"key1\",\n" + 
			"							\"expireSeconds\":10\n" +
			"              				\"values\":[\n" + 
			"                  				{\"value\":\"value1\"},\n" + 
			"                  				{\"value\":\"value2\"}\n" + 
			"              				]\n" + 
			"						},\n" + 
			"      					{\n" + 
			"              				\"key\":\"key2\",\n" + 
			"              				\"values\":[\n" + 
			"                  				{\"value\":\"value3\"},\n" + 
			"                  				{\"value\":\"value4\"}\n" + 
			"              				]\n" + 
			"						}\n" +
			"					   ]\n" +						
			"      		}\n" + 
			"]\n" + 
			"}";
	
	
	private static final String DATA_WITH_SORTSET_TYPES = "{\n" + 
			"\"data\":[	\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key\",\n" + 
			"                     \"values\":[\n" + 
			"                           {\"score\":1, \"value\":\"value1\" },{\"score\":2, \"value\":\"value2\" }]\n" + 
			"                 }]\n" + 
			"      		}\n" + 
			"]\n" + 
			"}";
	
	private static final String DATA_WITH_SORTSET_TYPES_WITH_TTL = "{\n" + 
			"\"data\":[	\n" + 
			"     		{\"sortset\": [{\n" + 
			"                     \"key\":\"key\",\n" + 
			"					  \"expireAtSeconds\":1293840000\n" +
			"                     \"values\":[\n" + 
			"                           {\"score\":1, \"value\":\"value1\" },{\"score\":2, \"value\":\"value2\" }]\n" + 
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
	
	private static final String DATA_WITH_HASH_TYPES_WITH_TTL = "{\n" + 
			"\"data\":[\n" + 
			"      		{\"hash\": [\n" + 
			"      					{\n" + 
			"      						\"key\":\"user\",\n" + 
			"					  		\"expireAtSeconds\":1293840000\n" +
			"      						\"values\":[\n" + 
			"      							{\"field\":\"name\", \"value\":\"alex\"},\n" + 
			"      							{\"field\":\"password\", \"value\":\"alex\"}\n" + 
			"      						]\n" + 
			"      					}\n" + 
			"      				]\n" + 
			"      		}\n" + 
			"		]\n" + 
			"}";
	
	@Test
	public void simple_types_should_be_set_into_redis() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_SIMPLE_TYPES.getBytes()));
		
		verify(jedis).set("key1".getBytes(), "value".getBytes());
		verify(jedis).set("key2".getBytes(), new byte[]{new Double(1.1).byteValue()});
		
	}
	
	@Test
	public void simple_types_should_be_set_into_redis_with_TTL() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_SIMPLE_TYPES_AND_TTL.getBytes()));
		
		verify(jedis).expire("key1".getBytes(), 10);
		verify(jedis).set("key1".getBytes(), "value".getBytes());
		verify(jedis).set("key2".getBytes(), new byte[]{new Double(1.1).byteValue()});
		
	}
	
	@Test
	public void list_types_should_be_rear_pushed_into_redis() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_LIST_TYPES.getBytes()));
		
		List<byte[]> expectedValuesKey1 = convertToByteArray("value1","value2");
		verify(jedis).rpush("key1".getBytes(), expectedValuesKey1.toArray(new byte[expectedValuesKey1.size()][]));
		
		List<byte[]> expectedValuesKey2 = convertToByteArray("value3","value4");
		verify(jedis).rpush("key2".getBytes(), expectedValuesKey2.toArray(new byte[expectedValuesKey1.size()][]));
		
	}
	
	@Test
	public void set_types_should_be_added_into_redis() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_SET_TYPES.getBytes()));
		
		List<byte[]> expectedValuesKey1 = convertToByteArray("value1","value2");
		verify(jedis).sadd("key1".getBytes(), expectedValuesKey1.toArray(new byte[expectedValuesKey1.size()][]));
		
		List<byte[]> expectedValuesKey2 = convertToByteArray("value3","value4");
		verify(jedis).sadd("key2".getBytes(), expectedValuesKey2.toArray(new byte[expectedValuesKey1.size()][]));
		
	}
	
	@Test
	public void set_types_should_be_added_into_redis_with_TTL() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_SET_TYPES_WITH_TTL.getBytes()));
		
		verify(jedis).expire("key1".getBytes(), 10);
		
		List<byte[]> expectedValuesKey1 = convertToByteArray("value1","value2");
		verify(jedis).sadd("key1".getBytes(), expectedValuesKey1.toArray(new byte[expectedValuesKey1.size()][]));
		
		List<byte[]> expectedValuesKey2 = convertToByteArray("value3","value4");
		verify(jedis).sadd("key2".getBytes(), expectedValuesKey2.toArray(new byte[expectedValuesKey1.size()][]));
		
	}
	
	@Test
	public void list_types_should_be_rear_pushed_into_redis_with_TTL() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_LIST_TYPES_WITH_TTL.getBytes()));
		
		verify(jedis).expire("key1".getBytes(), 10);
		
		List<byte[]> expectedValuesKey1 = convertToByteArray("value1","value2");
		verify(jedis).rpush("key1".getBytes(), expectedValuesKey1.toArray(new byte[expectedValuesKey1.size()][]));
		
		List<byte[]> expectedValuesKey2 = convertToByteArray("value3","value4");
		verify(jedis).rpush("key2".getBytes(), expectedValuesKey2.toArray(new byte[expectedValuesKey1.size()][]));
		
	}
	
	@Test
	public void hash_types_should_be_added_into_redis() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_HASH_TYPES.getBytes()));
		
		
		ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
		verify(jedis).hmset(eq("user".getBytes()), argument.capture());
		
		Map<byte[], byte[]> fieldsMap = argument.getValue();
		
		assertThat(fieldsMap, hasKey("name".getBytes()));
		assertThat(fieldsMap, hasKey("password".getBytes()));
		assertThat(fieldsMap, hasValue("alex".getBytes()));
	}
	
	@Test
	public void hash_types_should_be_added_into_redis_with_TTL() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_HASH_TYPES_WITH_TTL.getBytes()));
		
		verify(jedis).expireAt("user".getBytes(), 1293840000);
		
		ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
		verify(jedis).hmset(eq("user".getBytes()), argument.capture());
		
		Map<byte[], byte[]> fieldsMap = argument.getValue();
		
		assertThat(fieldsMap, hasKey("name".getBytes()));
		assertThat(fieldsMap, hasKey("password".getBytes()));
		assertThat(fieldsMap, hasValue("alex".getBytes()));
	}
	
	@Test
	public void sortset_types_should_be_added_into_redis() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_SORTSET_TYPES.getBytes()));
		
		ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
		verify(jedis).zadd(eq("key".getBytes()), argument.capture());

		byte[] objectValue1 = (byte[]) argument.getValue().get(1.0);
		assertThat(objectValue1, is("value1".getBytes()));
		byte[] objectValue2 = (byte[]) argument.getValue().get(2.0);
		assertThat(objectValue2, is("value2".getBytes()));
	}
	
	
	
	@Test
	public void sortset_types_should_be_added_into_redis_with_TTL() {
		
		DataReader dataReader = new DataReader(jedis);
		dataReader.read(new ByteArrayInputStream(DATA_WITH_SORTSET_TYPES_WITH_TTL.getBytes()));
		
		verify(jedis).expireAt("key".getBytes(), 1293840000);
		
		ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
		verify(jedis).zadd(eq("key".getBytes()), argument.capture());

		byte[] objectValue1 = (byte[]) argument.getValue().get(1.0);
		assertThat(objectValue1, is("value1".getBytes()));
		byte[] objectValue2 = (byte[]) argument.getValue().get(2.0);
		assertThat(objectValue2, is("value2".getBytes()));
	}
	
	private List<byte[]> convertToByteArray(String... values) {
		
		List<byte[]> bytes = new ArrayList<byte[]>();
		
		for (String value : values) {
			bytes.add(value.getBytes());
		}
		
		return bytes;
	}
	
}
