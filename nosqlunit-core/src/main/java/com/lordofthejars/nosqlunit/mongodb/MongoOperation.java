package com.lordofthejars.nosqlunit.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.core.IOUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public final class MongoOperation implements DatabaseOperation {

	private Mongo mongo;
	private MongoDbConfiguration mongoDbConfiguration;

	public MongoOperation(Mongo mongo, MongoDbConfiguration mongoDbConfiguration) {
		this.mongo = mongo;
		this.mongoDbConfiguration = mongoDbConfiguration;
	}

	@Override
	public void insert(String jsonData) {

		try {

			DBObject parsedData = parseData(jsonData);
			DB mongoDb = getMongoDb();

			insertParsedData(parsedData, mongoDb);

		} catch (IOException e) {
			throw new IllegalArgumentException(
					"Unexpected error reading data set file.", e);
		}

	}

	private void insertParsedData(DBObject parsedData, DB mongoDb) {
		Set<String> collectionaNames = parsedData.keySet();

		for (String collectionName : collectionaNames) {

			BasicDBList dataObjects = (BasicDBList) parsedData
					.get(collectionName);

			DBCollection dbCollection = mongoDb.getCollection(collectionName);

			for (Object dataObject : dataObjects) {
				dbCollection.insert((DBObject) dataObject);
			}

		}
	}

	@Override
	public void deleteAll() {
		DB mongoDb = getMongoDb();
		deleteAllElements(mongoDb);
	}

	private void deleteAllElements(DB mongoDb) {
		Set<String> collectionaNames = mongoDb.getCollectionNames();

		for (String collectionName : collectionaNames) {

			DBCollection dbCollection = mongoDb.getCollection(collectionName);
			dbCollection.remove(new BasicDBObject());
		}
	}

	@Override
	public void nonStrictAssertEquals(String expectedJsonData) {

		try {
			DBObject parsedData = parseData(expectedJsonData);
			MongoDbAssertion.nonStrictAssertEquals(parsedData, getMongoDb());
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"Unexpected error reading expected data set file.", e);
		}

	}

	@Override
	public void insertNotPresent(String jsonData) {
		
		try {
			DBObject parsedData = parseData(jsonData);
			DB mongoDb = getMongoDb();
			
			insertAllElementsNotInsertedBefore(parsedData, mongoDb);
			
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"Unexpected error reading expected data set file.", e);
		}
		
		
	}

	private void insertAllElementsNotInsertedBefore(DBObject parsedData,
			DB mongoDb) {
		Set<String> collectionaNames = parsedData.keySet();

		for (String collectionName : collectionaNames) {

			BasicDBList dataObjects = (BasicDBList) parsedData
					.get(collectionName);

			DBCollection dbCollection = mongoDb.getCollection(collectionName);

			for (Object dataObject : dataObjects) {
				DBObject dbObject = dbCollection.findOne((DBObject) dataObject);
				
				if(wasDbObjectNotInserted(dbObject)) {
					dbCollection.insert((DBObject)dataObject);
				}
				
			}

		}
	}

	private boolean wasDbObjectNotInserted(DBObject dbObject) {
		return dbObject == null;
	}
	
	private DBObject parseData(String jsonData) throws IOException {
		DBObject parsedData = (DBObject) JSON.parse(jsonData);
		return parsedData;
	}

	private DB getMongoDb() {

		DB db = mongo.getDB(this.mongoDbConfiguration.getDatabaseName());

		if (this.mongoDbConfiguration.isAuthenticateParametersSet()) {
			boolean authenticated = db.authenticate(
					this.mongoDbConfiguration.getUsername(),
					this.mongoDbConfiguration.getPassword().toCharArray());

			if (!authenticated) {
				throw new IllegalArgumentException(
						"Login/Password provided to connect to MongoDb are not valid");
			}

		}

		return db;
	}

}
