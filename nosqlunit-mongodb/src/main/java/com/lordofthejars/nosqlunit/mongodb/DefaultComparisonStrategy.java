package com.lordofthejars.nosqlunit.mongodb;

import java.io.IOException;
import java.io.InputStream;

import com.lordofthejars.nosqlunit.core.IOUtils;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.bson.Document;

public class DefaultComparisonStrategy implements MongoComparisonStrategy {

	@Override
	public boolean compare(MongoDbConnectionCallback connection, InputStream dataset) throws IOException {
		String expectedJsonData = loadContentFromInputStream(dataset);
		Document parsedData = parseData(expectedJsonData);

		MongoDbAssertion.strictAssertEquals(parsedData, connection.db());
		
		return true;
	}

	private String loadContentFromInputStream(InputStream inputStreamContent) throws IOException {
		return IOUtils.readFullStream(inputStreamContent);
	}
	
	private Document parseData(String jsonData) throws IOException {
		return Document.parse(jsonData);
	}

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
    }
	
}
