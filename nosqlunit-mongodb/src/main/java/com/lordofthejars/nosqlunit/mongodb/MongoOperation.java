package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import java.io.InputStream;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MongoOperation extends AbstractCustomizableDatabaseOperation<MongoDbConnectionCallback, Mongo> {

    private static Logger LOGGER = LoggerFactory.getLogger(MongoOptions.class);

    private Mongo mongo;

    private MongoDbConfiguration mongoDbConfiguration;

    protected MongoOperation(Mongo mongo, MongoDbConfiguration mongoDbConfiguration) {
        this.mongo = mongo;
        this.mongoDbConfiguration = mongoDbConfiguration;
        this.setInsertionStrategy(new DefaultInsertionStrategy());
        this.setComparisonStrategy(new DefaultComparisonStrategy());
    }

    public MongoOperation(MongoDbConfiguration mongoDbConfiguration) {
        try {
            this.mongo = mongoDbConfiguration.getMongo();
            this.mongo.setWriteConcern(mongoDbConfiguration.getWriteConcern());
            this.mongoDbConfiguration = mongoDbConfiguration;
            this.setInsertionStrategy(new DefaultInsertionStrategy());
            this.setComparisonStrategy(new DefaultComparisonStrategy());
        } catch (MongoException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void insert(InputStream contentStream) {

        insertData(contentStream);

    }

    private void insertData(InputStream contentStream) {
        try {

            final DB mongoDb = getMongoDb();
            executeInsertion(new MongoDbConnectionCallback() {

                @Override
                public DB db() {
                    return mongoDb;
                }
            }, contentStream);

        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading data set file.", e);
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

            if (isNotASystemCollection(collectionName)) {

                LOGGER.debug("Dropping Collection {}.", collectionName);

                DBCollection dbCollection = mongoDb.getCollection(collectionName);
                // Delete ALL, No DROP
                dbCollection.remove(new BasicDBObject(0));
            }
        }
    }

    private boolean isNotASystemCollection(String collectionName) {
        return !collectionName.startsWith("system.");
    }

    @Override
    public boolean databaseIs(InputStream contentStream) {

        return compareData(contentStream);

    }

    private boolean compareData(InputStream contentStream) throws NoSqlAssertionError {
        try {
            final DB mongoDb = getMongoDb();
            executeComparison(new MongoDbConnectionCallback() {

                @Override
                public DB db() {
                    return mongoDb;
                }
            }, contentStream);
            return true;
        } catch (NoSqlAssertionError e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading expected data set file.", e);
        }
    }

    private DB getMongoDb() {
        DB db = mongo.getDB(this.mongoDbConfiguration.getDatabaseName());
        return db;
    }

    @Override
    public Mongo connectionManager() {
        return mongo;
    }

}
