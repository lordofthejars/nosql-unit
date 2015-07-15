package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.IOUtils;
import com.mongodb.*;
import com.mongodb.util.JSON;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class DefaultInsertionStrategy implements MongoInsertionStrategy {

	private static final String SHARD_KEY_PATTERN = "shard-key-pattern";
	private static final String INDEXES = "indexes";
	private static final String INDEX = "index";
	private static final String INDEX_OPTIONS = "options";
	private static final String DATA = "data";
	private static final String DATABASE_COLLECTION_SEPARATOR = ".";

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

			if (isShardedCollection((DBObject) parsedData.get(collectionName))) {
				DBObject collection = (DBObject) parsedData.get(collectionName);
				insertShardKeyPattern(mongoDb, collectionName, collection);
			}

			if (isIndexes((DBObject) parsedData.get(collectionName))) {
				DBObject collection = (DBObject) parsedData.get(collectionName);
				insertIndexes(mongoDb, collectionName, collection);
			}

			insertCollection(parsedData, mongoDb, collectionName);

		}
	}

	private void insertCollection(DBObject parsedData, DB mongoDb, String collectionName) {
		BasicDBList data;
		if (isDataDirectly((DBObject) parsedData.get(collectionName))) { // Insert
			data = (BasicDBList) parsedData.get(collectionName);
		} else {
			DBObject collection = (DBObject) parsedData.get(collectionName);
			data = (BasicDBList) collection.get(DATA);
		}

		insertData(data, mongoDb, collectionName);
	}

	private void insertIndexes(DB mongoDb, String collectionName, DBObject collection) {
		DBCollection indexedCollection = mongoDb.getCollection(collectionName);
		BasicDBList indexes = (BasicDBList) collection.get(INDEXES);
		
		for (Object object : indexes) {
			DBObject index = (DBObject) object;
			
			DBObject indexKeys = (DBObject) index.get(INDEX);
			
			if(index.containsField(INDEX_OPTIONS)) {
				DBObject indexOptions = (DBObject)index.get(INDEX_OPTIONS);
				indexedCollection.createIndex(indexKeys, indexOptions);
			} else {
				indexedCollection.createIndex(indexKeys);
			}
		}
		
	}

	private void insertShardKeyPattern(DB mongoDb, String collectionName, DBObject collection) {
		String databaseName = mongoDb.getName();
		String collectionWithDatabase = databaseName + DATABASE_COLLECTION_SEPARATOR + collectionName;

		DBObject shardKeys = shardKeys(collection);
		MongoDbCommands.shardCollection(mongoDb.getMongo(), collectionWithDatabase, shardKeys);
	}

	private DBObject shardKeys(DBObject collection) {
		BasicDBList shards = (BasicDBList) collection.get(SHARD_KEY_PATTERN);
		BasicDBObjectBuilder shardKeysBuilder = new BasicDBObjectBuilder();

		for (Object dbObject : shards) {
			shardKeysBuilder.append(dbObject.toString(), 1);
		}

		DBObject shardKeys = shardKeysBuilder.get();
		return shardKeys;
	}

	private void insertData(BasicDBList dataObjects, DB mongoDb, String collectionName) {

		DBCollection dbCollection = mongoDb.getCollection(collectionName);

		for (Object dataObject : dataObjects) {
			for (String key : ((DBObject)dataObject).keySet()) {
        Object data = ((DBObject)dataObject).get(key);
        if (data instanceof DBRef) {
          ((DBObject)dataObject).put(key, new DBRef(((DBRef)data).getCollectionName(), ((DBRef)data).getId()));
        } else if(data instanceof BasicDBList) {
            for (Object subData : (BasicDBList) data) {
                for (String subKey : ((DBObject)data).keySet()) {
                    if(subData instanceof DBRef) {
                        ((BasicDBList)data).put(subKey, new DBRef(((DBRef)subData).getCollectionName(), ((DBRef)subData).getId()));
                    }
                }

            }
        }

      }
			dbCollection.insert((DBObject) dataObject);
		}
	}

	private boolean isDataDirectly(DBObject dbObject) {
		return dbObject instanceof BasicDBList;
	}

	private boolean isIndexes(DBObject dbObject) {
		if (!(dbObject instanceof BasicDBList)) {
			return dbObject.containsField(INDEXES);
		}
		// Means that data is present in current node.
		return false;
	}

	private boolean isShardedCollection(DBObject dbObject) {
		if (!(dbObject instanceof BasicDBList)) {
			return dbObject.containsField(SHARD_KEY_PATTERN);
		}
		// Means that data is present in current node.
		return false;
	}
}
