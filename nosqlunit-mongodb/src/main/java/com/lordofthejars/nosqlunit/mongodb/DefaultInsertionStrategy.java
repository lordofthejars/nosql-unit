package com.lordofthejars.nosqlunit.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.IOUtils;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class DefaultInsertionStrategy implements MongoInsertionStrategy {

	private static Logger LOGGER = LoggerFactory.getLogger(DefaultInsertionStrategy.class);
	
	@Override
	public void insert(MongoDbConnectionCallback connection, InputStream dataset) throws IOException {
		String jsonData = loadContentFromInputStream(dataset);
		DBObject parsedData = parseData(jsonData);

		insertParsedData(parsedData, connection.db());
	}

	private String loadContentFromInputStream(InputStream inputStreamContent) throws IOException {
		return IOUtils.readFullStream(inputStreamContent);
	}
	
	private DBObject parseData(String jsonData) throws IOException {
		DBObject parsedData = (DBObject) JSON.parse(jsonData);
		return parsedData;
	}
	
	private void insertParsedData(DBObject parsedData, DB mongoDb) {
		Set<String> collectionaNames = parsedData.keySet();

		for (String collectionName : collectionaNames) {

			BasicDBList dataObjects = (BasicDBList) parsedData.get(collectionName);

			DBCollection dbCollection = mongoDb.getCollection(collectionName);

			for (Object dataObject : dataObjects) {

				LOGGER.debug("Inserting {} To {}.", dataObject, dbCollection.getName());

				dbCollection.insert((DBObject) dataObject);
			}

		}
	}
}
