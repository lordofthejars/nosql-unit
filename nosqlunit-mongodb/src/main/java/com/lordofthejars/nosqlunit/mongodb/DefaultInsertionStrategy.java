package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.core.IOUtils;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
        Document parsedData = parseData(jsonData);

        insertParsedData(parsedData, connection.db(), connection.mongoClient());
    }

    private String loadContentFromInputStream(InputStream inputStreamContent) throws IOException {
        return IOUtils.readFullStream(inputStreamContent);
    }

    private Document parseData(String jsonData) throws IOException {
        Document parsedData = Document.parse(jsonData);
        return parsedData;
    }

    private void insertParsedData(Document parsedData, MongoDatabase mongoDb, MongoClient mongoClient) {
        Set<String> collectionaNames = parsedData.keySet();

        for (String collectionName : collectionaNames) {

            final Object document = parsedData.get(collectionName, Object.class);
            if (!isDataDirectly(document)) {
                Document doc = (Document) document;
                if (isShardedCollection(doc)) {
                    insertShardKeyPattern(mongoDb, mongoClient, collectionName, doc);
                }
                if (isIndexes(doc)) {
                    insertIndexes(mongoDb, collectionName, doc);
                }
            }

            insertCollection(parsedData, mongoDb, collectionName);

        }
    }

    private void insertCollection(Document parsedData, MongoDatabase mongoDb, String collectionName) {
        List<Document> data;
        if (isDataDirectly(parsedData.get(collectionName))) { // Insert
            data = parsedData.get(collectionName, List.class);
        } else {
            Document collection = parsedData.get(collectionName, Document.class);
            data = collection.get(DATA, List.class);
        }

        insertData(data, mongoDb, collectionName);
    }

    private void insertIndexes(MongoDatabase mongoDb, String collectionName, Document collection) {
        MongoCollection<Document> indexedCollection = mongoDb.getCollection(collectionName);
        List<Document> indexes = collection.get(INDEXES, List.class);

        for (Document index : indexes) {

            Document indexKeys = index.get(INDEX, Document.class);

            if (index.containsKey(INDEX_OPTIONS)) {
                Document indexOptions = index.get(INDEX_OPTIONS, Document.class);
                indexedCollection.createIndex(indexKeys, toIndexOptions(indexOptions));
            } else {
                indexedCollection.createIndex(indexKeys);
            }
        }

    }

    private IndexOptions toIndexOptions(Document indexOptions) {
        IndexOptions mongoDbIndexOptions = new IndexOptions();

        if (indexOptions.containsKey("background")) {
            mongoDbIndexOptions.background(indexOptions.getBoolean("background"));
        }

        if (indexOptions.containsKey("unique")) {
            mongoDbIndexOptions.unique(indexOptions.getBoolean("unique"));
        }

        if (indexOptions.containsKey("name")) {
            mongoDbIndexOptions.name(indexOptions.getString("name"));
        }

        if (indexOptions.containsKey("sparse")) {
            mongoDbIndexOptions.sparse(indexOptions.getBoolean("sparse"));
        }

        if (indexOptions.containsKey("expireAfterSeconds")) {
            mongoDbIndexOptions.expireAfter(indexOptions.getLong("expireAfterSeconds"), TimeUnit.SECONDS);
        }

        if (indexOptions.containsKey("version")) {
            mongoDbIndexOptions.version(indexOptions.getInteger("version"));
        }

        if (indexOptions.containsKey("weights")) {
            mongoDbIndexOptions.weights(indexOptions.get("weights", Bson.class));
        }

        if (indexOptions.containsKey("defaultLanguage")) {
            mongoDbIndexOptions.defaultLanguage(indexOptions.getString("defaultLanguage"));
        }

        if (indexOptions.containsKey("languageOverride")) {
            mongoDbIndexOptions.languageOverride(indexOptions.getString("languageOverride"));
        }

        if (indexOptions.containsKey("textVersion")) {
            mongoDbIndexOptions.textVersion(indexOptions.getInteger("textVersion"));
        }

        if (indexOptions.containsKey("sphereVersion")) {
            mongoDbIndexOptions.sphereVersion(indexOptions.getInteger("sphereVersion"));
        }

        if (indexOptions.containsKey("bits")) {
            mongoDbIndexOptions.bits(indexOptions.getInteger("bits"));
        }

        if (indexOptions.containsKey("min")) {
            mongoDbIndexOptions.min(indexOptions.getDouble("min"));
        }

        if (indexOptions.containsKey("max")) {
            mongoDbIndexOptions.max(indexOptions.getDouble("max"));
        }

        if (indexOptions.containsKey("bucketSize")) {
            mongoDbIndexOptions.bucketSize(indexOptions.getDouble("bucketSize"));
        }

        if (indexOptions.containsKey("storageEngine")) {
            mongoDbIndexOptions.storageEngine(indexOptions.get("storageEngine", Bson.class));
        }

        if (indexOptions.containsKey("partialFilterExpression")) {
            mongoDbIndexOptions.partialFilterExpression(indexOptions.get("partialFilterExpression", Bson.class));
        }

        return mongoDbIndexOptions;

    }

    private void insertShardKeyPattern(MongoDatabase mongoDb, MongoClient mongoClient, String collectionName, Document collection) {
        String databaseName = mongoDb.getName();
        String collectionWithDatabase = databaseName + DATABASE_COLLECTION_SEPARATOR + collectionName;

        Document shardKeys = shardKeys(collection);
        MongoDbCommands.shardCollection(mongoClient, collectionWithDatabase, shardKeys);
    }

    private Document shardKeys(Document collection) {
        List<Object> shards = collection.get(SHARD_KEY_PATTERN, List.class);
        Document document = new Document();

        for (Object dbObject : shards) {
            document.append(dbObject.toString(), 1);
        }
        return document;
    }

    private void insertData(List<Document> dataObjects, MongoDatabase mongoDb, String collectionName) {

        MongoCollection<Document> dbCollection = mongoDb.getCollection(collectionName);

        for (Document dataObject : dataObjects) {
            dbCollection.insertOne(dataObject);
        }
    }

    private boolean isDataDirectly(Object object) {
        return List.class.isAssignableFrom(object.getClass());
    }

    private boolean isIndexes(Document document) {
        return document.containsKey(INDEXES);
    }

    private boolean isShardedCollection(Document document) {
        return document.containsKey(SHARD_KEY_PATTERN);
    }
}
