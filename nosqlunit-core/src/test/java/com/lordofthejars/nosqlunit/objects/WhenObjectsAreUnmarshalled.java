package com.lordofthejars.nosqlunit.objects;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonParseException;
import org.junit.Test;

public class WhenObjectsAreUnmarshalled {

	private static final String SIMPLE_DATA = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"value\":\"alex\"			\n" + 
			"			}\n" + 
			"		]\n" + 
			"}\n" + 
			"";
	private static final String SIMPLE_DATA_INTEGER = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"value\":1			\n" + 
			"			}\n" + 
			"		]\n" + 
			"}\n" + 
			"";

	private static final String OBJECT_DATA = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"implementation\":\"com.lordofthejars.nosqlunit.objects.User\",\n" + 
			"				\"value\": {\n" + 
			"						\"login\":\"alex\",\n" + 
			"						\"password\":\"soto\"\n" + 
			"					 }			\n" + 
			"			}\n" + 
			"		]\n" + 
			"}";

	private static final String ARRAY_DATA = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"value\": [{\"value\":\"a\"},{\"value\":\"b\"}]			\n" + 
			"			}\n" + 
			"		]\n" + 
			"}";
	
	private static final String SET_DATA = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"implementation\":\"java.util.HashSet\",\n" + 
			"				\"value\": [{\"value\":\"a\"},{\"value\":\"b\"}]			\n" + 
			"			}\n" + 
			"		]\n" + 
			"}";
	
	private static final String ARRAY_OF_OBJECTS_DATA = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"implementation\":\"java.util.HashSet\",\n" + 
			"				\"value\": [\n" + 
			"						{\"implementation\":\"com.lordofthejars.nosqlunit.objects.User\", \"value\":{\"login\":\"alex\",\"password\":\"soto\"}}\n" + 
			"						,{\"implementation\":\"com.lordofthejars.nosqlunit.objects.User\", \"value\":{\"login\":\"ALEX\",\"password\":\"SOTO\"}}]			\n" + 
			"			}\n" + 
			"		]\n" + 
			"}";
	
	private static final String OBJECT_DATA_WITHOUT_IMPLEMENTATION = "{\n" + 
			"	\"data\": [\n" + 
			"			{\n" + 
			"				\"key\":\"key1\",\n" + 
			"				\"value\": {\n" + 
			"						\"login\":\"alex\",\n" + 
			"						\"password\":\"soto\"\n" + 
			"					 }			\n" + 
			"			}\n" + 
			"		]\n" + 
			"}";
	
	@Test
	public void native_data_should_be_read() throws JsonParseException, IOException, ClassNotFoundException {

		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		
		Map<String, Object> elements = keyValueObjectMapper.readValues(new ByteArrayInputStream(SIMPLE_DATA.getBytes()));
		String readElement = (String) elements.get("key1");
		assertThat(readElement, is("alex"));
	}
	
	@Test
	public void native_data_should_be_read_as_integer() throws JsonParseException, IOException, ClassNotFoundException {

		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		
		Map<String, Object> elements = keyValueObjectMapper.readValues(new ByteArrayInputStream(SIMPLE_DATA_INTEGER.getBytes()));
		Integer readElement = (Integer) elements.get("key1");
		assertThat(readElement, is(1));
	}
	
	@Test
	public void object_data_should_be_read_and_transformed_to_instance() {
		
		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		
		Map<String, Object> elements = keyValueObjectMapper.readValues(new ByteArrayInputStream(OBJECT_DATA.getBytes()));
		
		User readElement = (User)elements.get("key1");
		assertThat(readElement, is(new User("alex", "soto")));
		
	}

	@Test
	public void array_of_elements_should_be_inserted_into_ArrayList_if_no_implementation_provided() {
	
		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		
		Map<String, Object> elements = keyValueObjectMapper.readValues(new ByteArrayInputStream(ARRAY_DATA.getBytes()));
		List<String> readElements = (List<String>) elements.get("key1");
		
		assertThat(readElements, instanceOf(ArrayList.class));
		assertThat(readElements, containsInAnyOrder("a", "b"));
		
	}
	
	@Test
	public void array_of_elements_should_be_inserted_into_given_implementation_collection() {

		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		
		Map<String, Object> elements = keyValueObjectMapper.readValues(new ByteArrayInputStream(SET_DATA.getBytes()));
		Set<String> readElements = (Set<String>) elements.get("key1");
		
		assertThat(readElements, instanceOf(HashSet.class));
		assertThat(readElements, containsInAnyOrder("a", "b"));
	}
	
	@Test
	public void array_of_objects_should_be_read_and_tranformed_to_instance() {
		
		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		
		Map<String, Object> elements = keyValueObjectMapper.readValues(new ByteArrayInputStream(ARRAY_OF_OBJECTS_DATA.getBytes()));
		Set<User> readElements = (Set<User>) elements.get("key1");
		
		assertThat(readElements, containsInAnyOrder(new User("alex", "soto"), new User("ALEX", "SOTO")));
		
	}

	@Test(expected=IllegalArgumentException.class)
	public void an_exception_should_be_thrown_if_no_implementation_is_provided_in_objects() {
		
		KeyValueObjectMapper keyValueObjectMapper = new KeyValueObjectMapper();
		
		Map<String, Object> elements = keyValueObjectMapper.readValues(new ByteArrayInputStream(OBJECT_DATA_WITHOUT_IMPLEMENTATION.getBytes()));
	}
	
}
