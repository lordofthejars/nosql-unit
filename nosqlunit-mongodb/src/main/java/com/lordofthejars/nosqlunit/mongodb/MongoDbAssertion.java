package com.lordofthejars.nosqlunit.mongodb;

import java.util.HashSet;
import java.util.Set;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class MongoDbAssertion {

	private static final String SYSTEM_COLLECTIONS_PATTERN = "system."; 
	private static final String DATA = "data";
	
	private MongoDbAssertion() {
		super();
	}
	
	public static final void strictAssertEquals(DBObject expectedData, DB mongoDb) {
		Set<String> collectionaNames = expectedData.keySet();

		Set<String> mongodbCollectionNames = mongoDb.getCollectionNames();
		
		checkCollectionsName(collectionaNames, mongodbCollectionNames);
		
		for (String collectionName : collectionaNames) {
			
			checkCollectionObjects(expectedData, mongoDb, collectionaNames,
					collectionName);
			
		}
	}

	private static void checkCollectionsName(
			Set<String> expectedCollectionNames, Set<String> mongodbCollectionNames) {
		
		Set<String> mongoDbUserCollectionNames = getUserCollections(mongodbCollectionNames);
		
		Set<String> allCollections = new HashSet<String>(mongoDbUserCollectionNames);
		allCollections.addAll(expectedCollectionNames);
		
		if(allCollections.size() != expectedCollectionNames.size() || allCollections.size() != mongoDbUserCollectionNames.size()) {
			throw FailureHandler.createFailure("Expected collection names are %s but insert collection names are %s", expectedCollectionNames, mongoDbUserCollectionNames);
		}
		
	}

	private static Set<String> getUserCollections(
			Set<String> mongodbCollectionNames) {
		Set<String> mongoDbUserCollectionNames = new HashSet<String>();
		
		for (String mongodbCollectionName : mongodbCollectionNames) {
			
			if(isUserCollection(mongodbCollectionName)) {
				mongoDbUserCollectionNames.add(mongodbCollectionName);
			}
			
		}
		return mongoDbUserCollectionNames;
	}

	private static boolean isUserCollection(String mongodbCollectionName) {
		return !mongodbCollectionName.contains(SYSTEM_COLLECTIONS_PATTERN);
	}

	private static void checkCollectionObjects(DBObject expectedData,
			DB mongoDb, Set<String> collectionaNames, String collectionName)
			throws Error {
		DBObject object = (DBObject) expectedData.get(collectionName);
		BasicDBList	dataObjects = null;
		
		if(isShardedCollection(object)) {
			dataObjects = (BasicDBList)object.get(DATA);			
		} else {
			dataObjects = (BasicDBList)object;
		}
		
		DBCollection dbCollection = mongoDb.getCollection(collectionName);
		
		int expectedDataObjectsCount = dataObjects.size();
		long insertedDataObjectsCount = dbCollection.count();
		
		if(expectedDataObjectsCount != insertedDataObjectsCount) {
			throw FailureHandler.createFailure("Expected collection has %s elements but insert collection has %s", expectedDataObjectsCount, insertedDataObjectsCount);
		}
		
		for (Object dataObject : dataObjects) {
			
			DBObject expectedDataObject = (DBObject)dataObject;
			DBObject foundObject = dbCollection.findOne(expectedDataObject);
			
			
			
			if(!exists(foundObject)) {
				throw FailureHandler.createFailure("Object # %s # is not found into collection %s", expectedDataObject.toString(), collectionaNames);
			}

			checkSameKeys(expectedDataObject,foundObject);
			
		}
	}

	private static boolean isShardedCollection(DBObject dbObject) {
		return !(dbObject instanceof BasicDBList);
	}
	
	private static void checkSameKeys(DBObject expectedDataObject,DBObject foundObject) {
		
		Set<String> expectedKeys = expectedDataObject.keySet();
		Set<String> expectedNoneSystemKeys = noneSystemKeys(expectedKeys);
		Set<String> foundKeys = foundObject.keySet();
		Set<String> foundNoneSystemKeys = noneSystemKeys(foundKeys);

		Set<String> allKeys = new HashSet<String>(expectedNoneSystemKeys);
		allKeys.addAll(foundNoneSystemKeys);
		
		if(allKeys.size() != expectedNoneSystemKeys.size() || allKeys.size() != foundNoneSystemKeys.size()) {
			throw FailureHandler.createFailure("Expected DbObject and insert DbObject have different keys: Expected: %s Inserted: %s", expectedNoneSystemKeys, foundNoneSystemKeys);
		}
		
	}
	
	private static Set<String> noneSystemKeys(Set<String> keys) {
		
		Set<String> noneSystemKeys = new HashSet<String>();
		
		for (String key : keys) {
			if(!key.startsWith("_")) {
				noneSystemKeys.add(key);
			}
		}
		
		return noneSystemKeys;
	}
	
	private static boolean exists(DBObject foundObject) {
		return foundObject != null;
	}

	
}
