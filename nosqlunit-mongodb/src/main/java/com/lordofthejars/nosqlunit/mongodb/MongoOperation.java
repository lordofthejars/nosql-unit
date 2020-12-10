package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public final class MongoOperation extends AbstractCustomizableDatabaseOperation<MongoDbConnectionCallback, MongoClient> {

    private static Logger LOGGER = LoggerFactory.getLogger(MongoOperation.class);

    private MongoClient mongo;

    private MongoDbConfiguration mongoDbConfiguration;

    protected MongoOperation(MongoClient mongo, MongoDbConfiguration mongoDbConfiguration) {
        this.mongo = mongo;
        this.mongoDbConfiguration = mongoDbConfiguration;
        this.setInsertionStrategy(new DefaultInsertionStrategy());
        this.setComparisonStrategy(new DefaultComparisonStrategy());
    }

    public MongoOperation(MongoDbConfiguration mongoDbConfiguration) {
        try {
            this.mongo = mongoDbConfiguration.getMongo();
            //TODO
//            this.mongo.setWriteConcern(mongoDbConfiguration.getWriteConcern());
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

            final MongoDatabase mongoDb = getMongoDb();
            executeInsertion(new MongoDbConnectionCallback() {

                @Override
                public MongoDatabase db() {
                    return mongoDb;
                }

                @Override
                public MongoClient mongoClient() {
                    return mongo;
                }
            }, contentStream);

        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading data set file.", e);
        }
    }

    @Override
    public void deleteAll() {
        MongoDatabase mongoDb = getMongoDb();
        deleteAllElements(mongoDb);
    }

    private void deleteAllElements(MongoDatabase mongoDb) {
        final MongoIterable<String> listCollectionNames = mongoDb.listCollectionNames();

        for (String collectionName : listCollectionNames) {

            if (isNotASystemCollection(collectionName)) {

                LOGGER.debug("Dropping Collection {}.", collectionName);

                MongoCollection dbCollection = mongoDb.getCollection(collectionName);
                // Delete ALL, No DROP
                dbCollection.deleteMany(new Document());
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
            final MongoDatabase mongoDb = getMongoDb();
            executeComparison(new MongoDbConnectionCallback() {

                @Override
                public MongoDatabase db() {
                    return mongoDb;
                }

                @Override
                public MongoClient mongoClient() {
                    return mongo;
                }
            }, contentStream);
            return true;
        } catch (NoSqlAssertionError e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unexpected error reading expected data set file.", e);
        }
    }

    private MongoDatabase getMongoDb() {
        MongoDatabase db = mongo.getDatabase(this.mongoDbConfiguration.getDatabaseName());
        return db;
    }

    @Override
    public MongoClient connectionManager() {
        return mongo;
    }

}
