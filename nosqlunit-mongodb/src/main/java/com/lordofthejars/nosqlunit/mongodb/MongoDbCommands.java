package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.mongodb.replicaset.ConfigurationDocument;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Set;

public class MongoDbCommands {

    private static final String REPL_SET_GET_STATUS_COMMAND = "replSetGetStatus";
    private static final String REPL_SET_INITIATE_COMMAND = "replSetInitiate";
    private static final String RECONFIG_COMMAND = "replSetReconfig";
    private static final String ADD_SHARD_COMMAND = "addshard";
    private static final String ENABLE_SHARDING_COMMAND = "enablesharding";
    private static final String SHARD_COLLECTION_COMMAND = "shardcollection";
    private static final String LIST_SHARDS_COMMAND = "listShards";

    private MongoDbCommands() {
        super();
    }


    public static Document replicaSetGetStatus(MongoClient mongoClient) {
        return mongoClient.getDatabase("admin").runCommand(new Document(REPL_SET_GET_STATUS_COMMAND, 1));
    }

    public static Document shardCollection(MongoClient mongoClient, String collectionWithDatabase, Document shardKey) {
        MongoDatabase adminDb = getAdminDatabase(mongoClient);
        Document basicDBObject = new Document(SHARD_COLLECTION_COMMAND, collectionWithDatabase);
        basicDBObject.append("key", shardKey);

        return adminDb.runCommand(basicDBObject);
    }

    public static Document enableSharding(MongoClient mongoClient, String database) {
        MongoDatabase adminDb = getAdminDatabase(mongoClient);
        return adminDb.runCommand(new Document(ENABLE_SHARDING_COMMAND, database));
    }

    public static Document listShards(MongoClient mongoClient) {
        MongoDatabase adminDb = getAdminDatabase(mongoClient);
        return adminDb.runCommand(new Document(LIST_SHARDS_COMMAND, 1));
    }

    public static void addShard(MongoClient mongoClient, Set<String> shards) {
        MongoDatabase adminDb = getAdminDatabase(mongoClient);

        for (String shardUri : shards) {
            adminDb.runCommand(new Document(ADD_SHARD_COMMAND, shardUri));
        }
    }

    public static Document replicaSetInitiate(MongoClient mongoClient, ConfigurationDocument configurationDocument) {
        MongoDatabase adminDb = getAdminDatabase(mongoClient);
        Document command = new Document(REPL_SET_INITIATE_COMMAND,
                configurationDocument.getConfiguration());
        return adminDb.runCommand(command);
    }


    private static MongoDatabase getAdminDatabase(MongoClient mongoClient) {
        MongoDatabase adminDb = mongoClient.getDatabase("admin");
        return adminDb;
    }

    public static Document replSetReconfig(MongoClient mongoClient, ConfigurationDocument configurationDocument) {
        MongoDatabase adminDb = getAdminDatabase(mongoClient);
        Document command = new Document(RECONFIG_COMMAND,
                configurationDocument.getConfiguration());
        return adminDb.runCommand(command);
    }


    public static void shutdown(String host, int port) {
        MongoClient mongo = null;
        try {
            mongo = new MongoClient(host, port);
            DB db = mongo.getDB("admin");
            CommandResult shutdownResult = db.command(new BasicDBObject(
                    "shutdown", 1));
            shutdownResult.throwOnError();
        } catch (MongoException e) {
            //It is ok because response could not be returned because network connection is closed.
        } catch (Throwable e) {
            throw new IllegalStateException("Mongodb could not be shutdown.", e);
        } finally {
            mongo.close();
        }
    }
}
