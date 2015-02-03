package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class MongoDbAssertion {

	private static final String SYSTEM_COLLECTIONS_PATTERN = "system."; 
	private static final String DATA = "data";

    private static final Logger logger = LoggerFactory.getLogger(MongoDbAssertion.class);

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
		
		if(isShardOrIndexCollection(object)) {
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

	private static boolean isShardOrIndexCollection(DBObject dbObject) {
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

    //<editor-fold desc="Flexible comparator">

    /**
     * Checks that all the expected data is present in MongoDB.
     *
     * @param expectedData Expected data.
     * @param mongoDb      Mongo Database.
     */
    public static void flexibleAssertEquals(DBObject expectedData, DB mongoDb) {
        // Get the expected collections
        Set<String> collectionNames = expectedData.keySet();

        // Get the current collections in mongoDB
        Set<String> mongodbCollectionNames = mongoDb.getCollectionNames();

        // Check expected data
        flexibleCheckCollectionsName(collectionNames, mongodbCollectionNames);
        for (String collectionName : collectionNames) {
            flexibleCheckCollectionObjects(expectedData, mongoDb, collectionName);
        }
    }

    /**
     * Checks that all the expected collection names are present in MongoDB. Does not check that all the collection
     * names present in Mongo are in the expected dataset collection names.
     * <p/>
     * If any expected collection isn't found in the database collection, the returned error indicates only the
     * missing expected collections.
     *
     * @param expectedCollectionNames Expected collection names.
     * @param mongodbCollectionNames  Current MongoDB collection names.
     */
    private static void flexibleCheckCollectionsName(Set<String> expectedCollectionNames, Set<String> mongodbCollectionNames) {
        Set<String> mongoDbUserCollectionNames = getUserCollections(mongodbCollectionNames);

        boolean ok = true;
        HashSet<String> notFoundCollectionNames = new HashSet<String>();
        for (String expectedCollectionName : expectedCollectionNames) {
            if (!mongoDbUserCollectionNames.contains(expectedCollectionName)) {
                ok = false;
                notFoundCollectionNames.add(expectedCollectionName);
            }
        }

        if (!ok) {
            throw FailureHandler.createFailure("The following collection names %s were not found in the inserted collection names", notFoundCollectionNames);
        }
    }

    /**
     * Checks that each expected object in the collection exists in the database.
     *
     * @param expectedData   Expected data.
     * @param mongoDb        Mongo database.
     * @param collectionName Collection name.
     */
    private static void flexibleCheckCollectionObjects(DBObject expectedData, DB mongoDb, String collectionName) throws Error {
        DBObject object = (DBObject) expectedData.get(collectionName);
        BasicDBList dataObjects;

        if (isShardOrIndexCollection(object)) {
            dataObjects = (BasicDBList) object.get(DATA);
        } else {
            dataObjects = (BasicDBList) object;
        }

        DBCollection dbCollection = mongoDb.getCollection(collectionName);

        for (Object dataObject : dataObjects) {
            BasicDBObject expectedDataObject = (BasicDBObject) dataObject;
            DBObject filteredExpectedDataObject = filterProperties(expectedDataObject);
            DBObject foundObject = dbCollection.findOne(filteredExpectedDataObject);

            if (dbCollection.count(filteredExpectedDataObject) > 1) {
                logger.warn(String.format("There were found %d possible matches for this object # %s #. That could have been caused by ignoring too many properties.", dbCollection.count(filteredExpectedDataObject), expectedDataObject.toString()));
            }

            if (!exists(foundObject)) {
                throw FailureHandler.createFailure("Object # %s # is not found into collection %s", filteredExpectedDataObject.toString(), collectionName);
            }

            // Check same keys without filtering
            flexibleCheckSameKeys((DBObject) dataObject, foundObject);

        }
    }

    /**
     * Removes the properties set with "@IgnorePropertyValue" value from the dataObject.
     *
     * @param dataObject Object to filter.
     * @return Data object without the properties to be ignored.
     */
    private static BasicDBObject filterProperties(BasicDBObject dataObject) {
        BasicDBObject filteredDataObject = new BasicDBObject();

        for (Map.Entry<String, Object> entry : dataObject.entrySet()) {
            if (!(entry.getValue() instanceof String
                    && entry.getValue().equals("@IgnorePropertyValue"))) {
                filteredDataObject.put(entry.getKey(), entry.getValue());
            }
        }

        return filteredDataObject;
    }

    /**
     * Checks that all the properties are present in both expected and database objects, even the ignored properties.
     * <p/>
     * When there are differences between both objects keys, the returned error indicates only the keys that
     * are missing in each object.
     *
     * @param expectedDataObject Expected object.
     * @param foundObject        Database object.
     */
    private static void flexibleCheckSameKeys(DBObject expectedDataObject, DBObject foundObject) {
        Set<String> expectedKeys = expectedDataObject.keySet();
        Set<String> expectedNoneSystemKeys = noneSystemKeys(expectedKeys);
        Set<String> foundKeys = foundObject.keySet();
        Set<String> foundNoneSystemKeys = noneSystemKeys(foundKeys);

        Set<String> allKeys = new HashSet<String>(expectedNoneSystemKeys);
        allKeys.addAll(foundNoneSystemKeys);

        HashSet<String> expectedKeysNotInserted = new HashSet<String>();
        HashSet<String> insertedKeysNotExpected = new HashSet<String>();

        for (String key : allKeys) {
            if (!expectedNoneSystemKeys.contains(key)) {
                insertedKeysNotExpected.add(key);
            }
            if (!foundNoneSystemKeys.contains(key)) {
                expectedKeysNotInserted.add(key);
            }
        }

        if (expectedKeysNotInserted.size() > 0 || insertedKeysNotExpected.size() > 0) {
            StringBuilder errorMessage = new StringBuilder("Expected DbObject and insert DbObject have different keys: ");
            if (expectedKeysNotInserted.size() > 0) {
                errorMessage.append("expected keys not inserted ").append(expectedKeysNotInserted).append(" ");
            }
            if (insertedKeysNotExpected.size() > 0) {
                errorMessage.append("inserted keys not expected ").append(insertedKeysNotExpected).append(" ");
            }

            throw FailureHandler.createFailure(errorMessage.toString());
        }
    }
    //</editor-fold desc="Flexible comparator">
}
