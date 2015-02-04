package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
    public static void flexibleAssertEquals(DBObject expectedData, String[] ignorePropertyValues, DB mongoDb) {
        // Get the expected collections
        Set<String> collectionNames = expectedData.keySet();

        // Get the current collections in mongoDB
        Set<String> mongodbCollectionNames = mongoDb.getCollectionNames();

        // Get the concrete property names that should be ignored
        // Map<String:Collection, Set<String:Property>>
        Map<String, Set<String>> propertiesToIgnore = parseIgnorePropertyValues(collectionNames, ignorePropertyValues);

        // Check expected data
        flexibleCheckCollectionsName(collectionNames, mongodbCollectionNames);
        for (String collectionName : collectionNames) {
            flexibleCheckCollectionObjects(expectedData, mongoDb, collectionName, propertiesToIgnore);
        }
    }

    /**
     * Resolve the properties that will be ignored for each expected collection.
     * <p/>
     * Parses the input value following the rules for valid collection and property names
     * defined in <a href="http://docs.mongodb.org/manual/reference/limits/#naming-restrictions>
     * "Mongo DB: naming restrictions"</a> document.
     *
     * @param ignorePropertyValues Input values defined with @IgnorePropertyValue.
     * @return Map with the properties that will be ignored for each document.
     */
    private static Map<String, Set<String>> parseIgnorePropertyValues(Set<String> collectionNames, String[] ignorePropertyValues) {
        Map<String, Set<String>> propertiesToIgnore = new HashMap<String, Set<String>>();
        Pattern collectionAndPropertyPattern = Pattern.compile("^(?!system\\.)([a-z,A-Z,_][^$\0]*)([.])([^$][^.\0]*)$");
        Pattern propertyPattern = Pattern.compile("^([^$][^.0]*)$");

        for (String ignorePropertyValue : ignorePropertyValues) {
            Matcher collectionAndPropertyMatcher = collectionAndPropertyPattern.matcher(ignorePropertyValue);
            Matcher propertyMatcher = propertyPattern.matcher(ignorePropertyValue);

            // If the property to ignore includes the collection, add it to only exclude
            // the property in the indicated collection
            if (collectionAndPropertyMatcher.matches()) {
                // Add the property to ignore to the proper collection
                String collectionName = collectionAndPropertyMatcher.group(1);
                String propertyName = collectionAndPropertyMatcher.group(3);

                if (collectionNames.contains(collectionName)) {
                    Set<String> properties = propertiesToIgnore.get(collectionName);
                    if (properties == null) {
                        properties = new HashSet<String>();
                    }
                    properties.add(propertyName);
                    propertiesToIgnore.put(collectionName, properties);
                } else {
                    logger.warn(String.format("Collection %s for %s is not defined as expected. It won't be used for ignoring properties", collectionName, ignorePropertyValue));
                }
                // If the property to ignore doesn't include the collection, add it to
                // all the expected collections
            } else if (propertyMatcher.matches()) {
                String propertyName = propertyMatcher.group(0);

                // Add the property to ignore to all the expected collections
                for (String collectionName : collectionNames) {
                    Set<String> properties = propertiesToIgnore.get(collectionName);
                    if (properties == null) {
                        properties = new HashSet<String>();
                    }
                    properties.add(propertyName);
                    propertiesToIgnore.put(collectionName, properties);
                }
                // If doesn't match any pattern
            } else {
                logger.warn(String.format("Property %s has an invalid collection.property value. It won't be used for ignoring properties", ignorePropertyValue));
            }
        }

        return propertiesToIgnore;
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
    private static void flexibleCheckCollectionObjects(DBObject expectedData, DB mongoDb, String collectionName, Map<String, Set<String>> propertiesToIgnore) throws Error {
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
            DBObject filteredExpectedDataObject = filterProperties(expectedDataObject, propertiesToIgnore.get(collectionName));
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
     * Removes the properties defined with @IgnorePropertyValue annotation.
     *
     * @param dataObject Object to filter.
     * @param propertiesToIgnore Properties to filter
     * @return Data object without the properties to be ignored.
     */
    private static BasicDBObject filterProperties(BasicDBObject dataObject, Set<String> propertiesToIgnore) {
        BasicDBObject filteredDataObject = new BasicDBObject();

        for (Map.Entry<String, Object> entry : dataObject.entrySet()) {
            if (propertiesToIgnore == null || !propertiesToIgnore.contains(entry.getKey())) {
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
