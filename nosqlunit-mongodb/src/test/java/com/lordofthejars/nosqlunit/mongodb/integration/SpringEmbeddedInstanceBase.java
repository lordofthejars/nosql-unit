package com.lordofthejars.nosqlunit.mongodb.integration;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.client.MongoClient;
import org.junit.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import static com.lordofthejars.nosqlunit.mongodb.MongoDbRule.MongoDbRuleBuilder.newMongoDbRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public abstract class SpringEmbeddedInstanceBase
{
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private MongoClient mongo;

    @Rule
    public MongoDbRule mongoDbRule = newMongoDbRule().defaultSpringMongoDb("test");

    protected void validateMongoConnection()
    {
        DatabaseOperation<MongoClient> databaseOperation = mongoDbRule.getDatabaseOperation();
        MongoClient connectionManager = databaseOperation.connectionManager();

        assertThat(connectionManager, is(mongo));
    }

}
