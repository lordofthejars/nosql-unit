package com.lordofthejars.nosqlunit.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.IOUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class DefaultInsertionStrategy implements MongoInsertionStrategy {

	private static final String SHARD_KEY_PATTERN = "shard-key-pattern";
	private static final String DATA = "data";
	private static final String DATABASE_COLLECTION_SEPARATOR = ".";

	@Override
	public void insert(MongoDbConnectionCallback connection, InputStream dataset)
			throws IOException {
		String jsonData = loadContentFromInputStream(dataset);
		DBObject parsedData = parseData(jsonData);

		insertParsedData(parsedData, connection.db());
	}

	private String loadContentFromInputStream(InputStream inputStreamContent)
			throws IOException {
		return IOUtils.readFullStream(inputStreamContent);
	}

	private DBObject parseData(String jsonData) throws IOException {
		DBObject parsedData = (DBObject) JSON.parse(jsonData);
		return parsedData;
	}

	private void insertParsedData(DBObject parsedData, DB mongoDb) {
		Set<String> collectionaNames = parsedData.keySet();

		for (String collectionName : collectionaNames) {

			if (isShardedCollection((DBObject) parsedData.get(collectionName))) {

				DBObject collection = (DBObject)parsedData.get(collectionName);
				
				//Insert shard-key-pattern
				insertShardKeyPattern(mongoDb, collectionName, collection);
				
				//Insert data
				BasicDBList data = (BasicDBList)collection.get(DATA);
				insertData(data, mongoDb, collectionName);
			
			} else {
				BasicDBList dataObjects = (BasicDBList) parsedData
						.get(collectionName);
				insertData(dataObjects, mongoDb, collectionName);
			}
		}
	}

	private void insertShardKeyPattern(DB mongoDb, String collectionName,
			DBObject collection) {
		String databaseName = mongoDb.getName();
		String collectionWithDatabase = databaseName+DATABASE_COLLECTION_SEPARATOR+collectionName;
		
		DBObject shardKeys = shardKeys(collection);
		MongoDbCommands.shardCollection(mongoDb.getMongo(), collectionWithDatabase, shardKeys);
	}

	private DBObject shardKeys(DBObject collection) {
		BasicDBList shards = (BasicDBList)collection.get(SHARD_KEY_PATTERN);
		BasicDBObjectBuilder shardKeysBuilder = new BasicDBObjectBuilder();
		
		for (Object dbObject : shards) {
			shardKeysBuilder.append(dbObject.toString(), 1);
		}
		
		DBObject shardKeys = shardKeysBuilder.get();
		return shardKeys;
	}

	private void insertData(BasicDBList dataObjects, DB mongoDb,
			String collectionName) {

		DBCollection dbCollection = mongoDb
				.getCollection(collectionName);

		for (Object dataObject : dataObjects) {

			dbCollection.insert((DBObject) dataObject);
		}
	}

	private boolean isShardedCollection(DBObject dbObject) {
		return !(dbObject instanceof BasicDBList);
	}
}
