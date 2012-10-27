package com.lordofthejars.nosqlunit.hbase.model;

import java.io.IOException;
import java.io.InputStream;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonDataSetParser implements DataSetParser {

	public ParsedDataModel parse(InputStream inputStream) {
		
		ObjectMapper mapper = new ObjectMapper();
		
		try {
			return mapper.readValue(inputStream, ParsedDataModel.class);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException(e);
		} catch (JsonMappingException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
		
	}

}
