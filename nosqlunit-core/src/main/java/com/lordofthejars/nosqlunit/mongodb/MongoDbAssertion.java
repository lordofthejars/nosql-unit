package com.lordofthejars.nosqlunit.mongodb;

import java.util.Set;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoDbAssertion {

	private MongoDbAssertion() {
		super();
	}
	
	public static final void nonStrictAssertEquals(DBObject expectedData, DB mongoDb) {
		Set<String> collectionaNames = expectedData.keySet();

		for (String collectionName : collectionaNames) {
			
			BasicDBList dataObjects = (BasicDBList)expectedData.get(collectionName);
			
			DBCollection dbCollection = mongoDb.getCollection(collectionName);
			
			for (Object dataObject : dataObjects) {
				DBObject foundObject = dbCollection.findOne((DBObject)dataObject);
				
				if(!exists(foundObject)) {
					throw FailureHandler.createFailure("Object # %s # is not found into collection %s", dataObject.toString(), collectionaNames);
				}
				
			}
			
		}
	}
	

	private static boolean exists(DBObject foundObject) {
		return foundObject != null;
	}

	
}
