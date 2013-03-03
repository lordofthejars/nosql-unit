package com.lordofthejars.nosqlunit.neo4j.extension.springtemplate;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class DataParser {

	private static final String IMPLEMENTATION_TOKEN = "implementation";
	private static final String DATA_TOKEN = "data";
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public List<Object> readValues(InputStream dataStream) {

		List<Object> definedObjects = new ArrayList<Object>();

		try {
			
			Iterator<JsonNode> elements = dataElements(dataStream);
			
			while (elements.hasNext()) {
				JsonNode definedObject = elements.next();

				String implementationValue = getImplementationValue(definedObject);
				JsonNode objectDefinition = getObjectDefinition(definedObject);

				Object unmarshallObject = unmarshallObject(objectDefinition, implementationValue);
				definedObjects.add(unmarshallObject);
			}
			
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}

		return definedObjects;
	}

	private Object unmarshallObject(JsonNode value, String implementationValue) throws IOException, JsonParseException,
			JsonMappingException, ClassNotFoundException {
		return OBJECT_MAPPER.readValue(value, Class.forName(implementationValue));
	}

	private JsonNode getObjectDefinition(JsonNode definedObject) {
		JsonNode object = definedObject.get("object");

		if (object != null) {
			return object;
		} else {
			throw new IllegalArgumentException("Object token should be used for defining object properties.");
		}

	}

	private String getImplementationValue(JsonNode definedObject) {
		JsonNode implementationNode = definedObject.get(IMPLEMENTATION_TOKEN);

		if (implementationNode != null) {
			return implementationNode.getValueAsText();
		} else {
			throw new IllegalArgumentException("No implementation class has been provided.");
		}
	}

	private Iterator<JsonNode> dataElements(InputStream dataStream) throws IOException, JsonParseException,
			JsonMappingException {
		JsonNode rootNode = OBJECT_MAPPER.readValue(dataStream, JsonNode.class);
		JsonNode dataNode = rootNode.path(DATA_TOKEN);

		Iterator<JsonNode> elements = dataNode.getElements();
		return elements;
	}

}
