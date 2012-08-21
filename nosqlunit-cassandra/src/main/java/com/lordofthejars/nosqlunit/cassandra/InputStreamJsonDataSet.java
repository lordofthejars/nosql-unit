package com.lordofthejars.nosqlunit.cassandra;

import java.io.IOException;
import java.io.InputStream;

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.dataset.commons.AbstractCommonsParserDataSet;
import org.cassandraunit.dataset.commons.ParsedKeyspace;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class InputStreamJsonDataSet extends AbstractCommonsParserDataSet implements DataSet  {

	private InputStream inputDataSetLocation;
	
	public InputStreamJsonDataSet(InputStream inputDataSetLocation) {
		this.inputDataSetLocation = inputDataSetLocation;
	}
	
	@Override
	protected ParsedKeyspace getParsedKeyspace() {
		if (inputDataSetLocation == null) {
			throw new ParseException("Dataset not found");
		}

		ObjectMapper jsonMapper = new ObjectMapper();
		try {
			return jsonMapper.readValue(inputDataSetLocation, ParsedKeyspace.class);
		} catch (JsonParseException e) {
			throw new ParseException(e);
		} catch (JsonMappingException e) {
			throw new ParseException(e);
		} catch (IOException e) {
			throw new ParseException(e);
		}
	}

	
}
