package com.lordofthejars.nosqlunit.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

import java.util.ArrayList;
import java.util.List;

public class ReplicationMongoDbConfigurationBuilder {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 27017;

    private MongoDbConfiguration mongoDbConfiguration = new MongoDbConfiguration();
    private List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();

    private boolean enableSharding = false;

    private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

    private ReplicationMongoDbConfigurationBuilder() {
        super();
    }

    public static ReplicationMongoDbConfigurationBuilder replicationMongoDbConfiguration() {
        return new ReplicationMongoDbConfigurationBuilder();
    }

    public ReplicationMongoDbConfigurationBuilder seed(String host, int port) {
        serverAddresses.add(new ServerAddress(host, port));
        return this;
    }

    public ReplicationMongoDbConfigurationBuilder databaseName(
            String databaseName) {
        mongoDbConfiguration.setDatabaseName(databaseName);
        return this;
    }

    public ReplicationMongoDbConfigurationBuilder username(String username) {
        mongoDbConfiguration.setUsername(username);
        return this;
    }

    public ReplicationMongoDbConfigurationBuilder password(String password) {
        mongoDbConfiguration.setPassword(password);
        return this;
    }

    public ReplicationMongoDbConfigurationBuilder connectionIdentifier(
            String identifier) {
        mongoDbConfiguration.setConnectionIdentifier(identifier);
        return this;
    }

    public ReplicationMongoDbConfigurationBuilder enableSharding() {
        this.enableSharding = true;
        return this;
    }

    public ReplicationMongoDbConfigurationBuilder writeConcern(
            WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    public MongoDbConfiguration configure() {

        if (this.serverAddresses.isEmpty()) {
            addDefaultSeed();
        }


        MongoClient mongoClient = new MongoClient(this.serverAddresses);

        if (this.enableSharding) {
            enableSharding(mongoClient);
        }

        mongoDbConfiguration.setWriteConcern(writeConcern);
        mongoDbConfiguration.setMongo(mongoClient);

        return mongoDbConfiguration;
    }

    private void enableSharding(MongoClient mongoClient) {

        MongoDbCommands.enableSharding(mongoClient,
                this.mongoDbConfiguration.getDatabaseName());
    }

    private boolean isAuthenticationSet() {
        return this.mongoDbConfiguration.getUsername() != null
                && this.mongoDbConfiguration.getPassword() != null;
    }

    private void addDefaultSeed() {
        serverAddresses.add(new ServerAddress(DEFAULT_HOST, DEFAULT_PORT));
    }

}
