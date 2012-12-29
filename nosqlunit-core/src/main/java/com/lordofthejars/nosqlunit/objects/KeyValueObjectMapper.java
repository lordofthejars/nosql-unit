package com.lordofthejars.nosqlunit.objects;

import static org.joor.Reflect.on;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class KeyValueObjectMapper {

	private static final String NO_IMPLEMENTATION_PROVIDED = "";
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public Map<Object, Object> readValues(InputStream dataStream) {

		Map<Object, Object> objects = new HashMap<Object, Object>();

		try {

			Iterator<JsonNode> elements = dataElements(dataStream);

			objects.putAll(readElements(elements));

		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Implementation class is not found in classpath", e);
		}

		return objects;

	}

	private Map<Object, Object> readElements(Iterator<JsonNode> elements) throws IOException,
			JsonParseException, JsonMappingException, ClassNotFoundException {
		
		Map<Object, Object> objects = new HashMap<Object, Object>();
		
		while (elements.hasNext()) {
			JsonNode element = elements.next();
			
			Object keyValue = keyValue(element);
			Object readValue = readValue(element);
			
			objects.put(keyValue, readValue);
		}
		
		return objects;
	}

	private Object readValue(JsonNode element) throws IOException, JsonParseException, JsonMappingException,
			ClassNotFoundException {


		JsonNode value = valueNode(element);
		String implementationValue = implementationClass(element);

		Object readObject = readElement(value, implementationValue);

		return readObject;
	}

	private Object readElement(JsonNode value, String implementationValue) throws IOException, JsonParseException,
			JsonMappingException, ClassNotFoundException {
		Object readObject = null;
		if (value.isArray()) {
			readObject = readArray(value, implementationValue);
		} else {
			readObject = readObject(value, implementationValue);
		}
		return readObject;
	}

	private Object readObject(JsonNode value, String implementationValue) throws IOException, JsonParseException,
			JsonMappingException, ClassNotFoundException {
		Object readObject;
		if (value.isObject()) {
			readObject = unmarshallObject(value, implementationValue);
		} else {
			readObject = simpleValue(value);
		}
		return readObject;
	}

	private Object unmarshallObject(JsonNode value, String implementationValue) throws IOException, JsonParseException,
			JsonMappingException, ClassNotFoundException {
	
		if(NO_IMPLEMENTATION_PROVIDED.equals(implementationValue)) {
			throw new IllegalArgumentException("No implementation class has been provided.");
		}
		
		return OBJECT_MAPPER.readValue(value, Class.forName(implementationValue));
	}

	private Object readArray(JsonNode value, String implementationValue) throws IOException, JsonParseException,
			JsonMappingException, ClassNotFoundException {
		Object readObject;
		Iterator<JsonNode> elements = value.getElements();

		Collection<Object> objects = collection(implementationValue);
		
		while (elements.hasNext()) {
			JsonNode newElement = elements.next();
			objects.add(readValue(newElement));
		}

		readObject = objects;
		return readObject;
	}

	private String implementationKeyClass(JsonNode element) {
		String implementationValue = NO_IMPLEMENTATION_PROVIDED;

		if (element.has(KeyValueTokens.IMPLEMENTATION_KEY_TOKEN)) {
			implementationValue = element.path(KeyValueTokens.IMPLEMENTATION_KEY_TOKEN).getTextValue();
		}
		return implementationValue.trim();
	}
	
	private String implementationClass(JsonNode element) {
		String implementationValue = NO_IMPLEMENTATION_PROVIDED;

		if (element.has(KeyValueTokens.IMPLEMENTATION_TOKEN)) {
			implementationValue = element.path(KeyValueTokens.IMPLEMENTATION_TOKEN).getTextValue();
		}
		return implementationValue.trim();
	}

	private JsonNode valueNode(JsonNode element) {
		if (element.has(KeyValueTokens.VALUE_TOKEN)) {
			return element.path(KeyValueTokens.VALUE_TOKEN);
		} else {
			throw new IllegalArgumentException("Given dataset does not contain "+KeyValueTokens.VALUE_TOKEN+" token.");
		}
	}

	private Collection<Object> collection(String implementation) {
		if(implementation.equals(NO_IMPLEMENTATION_PROVIDED)) {
			return new ArrayList<Object>();
		} else {
			return on(implementation).create().get();
		}
	}
	
	private Object simpleValue(JsonNode simpleValue) {
		if (simpleValue.isNumber()) {
			switch (simpleValue.getNumberType()) {
			case BIG_DECIMAL:
				return simpleValue.getDecimalValue();
			case BIG_INTEGER:
				return simpleValue.getBigIntegerValue();
			case DOUBLE:
				return simpleValue.getDoubleValue();
			case FLOAT:
				return simpleValue.getDoubleValue();
			case INT:
				return simpleValue.getIntValue();
			case LONG:
				return simpleValue.getLongValue();
			default:
				return simpleValue.getTextValue();
			}
		} else {
			return simpleValue.getTextValue();
		}
	}
	
	private Object keyValue(JsonNode element) throws JsonParseException, JsonMappingException, IOException, ClassNotFoundException {
		Object keyValue;
		if (element.has(KeyValueTokens.KEY_TOKEN)) {
			String implementationKey = implementationKeyClass(element);
			keyValue = readElement(element.path(KeyValueTokens.KEY_TOKEN), implementationKey);
		} else {
			throw new IllegalArgumentException("Given dataset does not contain "+KeyValueTokens.KEY_TOKEN+" token.");
		}
		return keyValue;
	}

	private Iterator<JsonNode> dataElements(InputStream dataStream) throws IOException, JsonParseException,
			JsonMappingException {
		JsonNode rootNode = OBJECT_MAPPER.readValue(dataStream, JsonNode.class);
		JsonNode dataNode = rootNode.path(KeyValueTokens.DATA_TOKEN);

		Iterator<JsonNode> elements = dataNode.getElements();
		return elements;
	}

}
