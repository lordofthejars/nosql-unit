package com.lordofthejars.nosqlunit.mongodb;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDb.InMemoryMongoRuleBuilder.newInMemoryMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDbConfigurationBuilder.inMemoryMongoDb;

import java.io.ByteArrayInputStream;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class WhenEmbeddedMongoDbOperationsArRequired {

    private static final String DATA = "" +
            "{" +
            "\"collection1\": " +
            "	[" +
            "		{\"id\":1,\"code\":\"JSON dataset\",}" +
            "	]" +
            "}";

    @ClassRule
    public static final InMemoryMongoDb IN_MEMORY_MONGO_DB = newInMemoryMongoDbRule().build();

    @After
    public void tearDown() {
        Mongo defaultEmbeddedInstance = EmbeddedMongoInstancesFactory.getInstance().getDefaultEmbeddedInstance();
        defaultEmbeddedInstance.getDB("test").getCollection("collection1").drop();
    }

    @Test
    public void data_should_be_inserted_into_mongodb() {

        MongoOperation mongoOperation = new MongoOperation(inMemoryMongoDb().databaseName("test").build());
        mongoOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        Mongo mongo = mongoOperation.connectionManager();
        DBCollection collection = mongo.getDB("test").getCollection("collection1");
        DBObject object = collection.findOne();

        assertThat((Integer) object.get("id"), is(new Integer(1)));
        assertThat((String) object.get("code"), is("JSON dataset"));
    }

    @Test
    public void data_should_be_removed_from_mongo() {

        MongoOperation mongoOperation = new MongoOperation(inMemoryMongoDb().databaseName("test").build());
        mongoOperation.insert(new ByteArrayInputStream(DATA.getBytes()));
        mongoOperation.deleteAll();

        Mongo mongo = mongoOperation.connectionManager();
        DBCollection collection = mongo.getDB("test").getCollection("collection1");
        DBObject object = collection.findOne();

        assertThat(object, is(nullValue()));
    }

    @Test
    public void data_should_be_compared_between_expected_and_current_data() {

        MongoOperation mongoOperation = new MongoOperation(inMemoryMongoDb().databaseName("test").build());
        mongoOperation.insert(new ByteArrayInputStream(DATA.getBytes()));

        boolean result = mongoOperation.databaseIs(new ByteArrayInputStream(DATA.getBytes()));

        assertThat(result, is(true));
    }


}
