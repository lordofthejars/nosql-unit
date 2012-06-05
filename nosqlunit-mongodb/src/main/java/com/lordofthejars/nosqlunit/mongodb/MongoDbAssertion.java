package com.lordofthejars.nosqlunit.mongodb;

import java.util.HashSet;
import java.util.Set;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/*
 * TODO Guice
 */
public class MongoDbAssertion {

	private static final String SYSTEM_COLLECTIONS_PATTERN = "system."; 
	
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
		BasicDBList dataObjects = (BasicDBList)expectedData.get(collectionName);
		
		DBCollection dbCollection = mongoDb.getCollection(collectionName);
		
		int expectedDataObjectsCount = dataObjects.size();
		long insertedDataObjectsCount = dbCollection.count();
		
		if(expectedDataObjectsCount != insertedDataObjectsCount) {
			throw FailureHandler.createFailure("Expected collection has %s elements but insert collection has %s", expectedDataObjectsCount, insertedDataObjectsCount);
		}
		
		for (Object dataObject : dataObjects) {
			DBObject foundObject = dbCollection.findOne((DBObject)dataObject);
			
			if(!exists(foundObject)) {
				throw FailureHandler.createFailure("Object # %s # is not found into collection %s", dataObject.toString(), collectionaNames);
			}
			
		}
	}

	private static boolean exists(DBObject foundObject) {
		return foundObject != null;
	}

	
}
