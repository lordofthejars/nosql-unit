package com.lordofthejars.nosqlunit.demo.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbRule.MongoDbRuleBuilder.newMongoDbRule;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.demo.model.Book;
import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;

public class WhenANewBookIsCreated {

    @ClassRule
    public static ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo").appendSingleCommandLineArguments("-vvv")
            .build();

    @Rule
    public MongoDbRule remoteMongoDbRule = newMongoDbRule().defaultManagedMongoDb("test");

    @Test
    @UsingDataSet(locations = "initialData.json", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
    @ShouldMatchDataSet(location = "expectedData.json")
    public void book_should_be_inserted_into_repository() {

        BookManager bookManager = new BookManager(MongoDbUtil.getCollection(Book.class.getSimpleName()));

        Book book = new Book("The Lord Of The Rings", 1299);
        bookManager.create(book);

    }

}
