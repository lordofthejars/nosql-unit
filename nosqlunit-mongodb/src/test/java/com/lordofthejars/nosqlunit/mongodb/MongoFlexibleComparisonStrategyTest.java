package com.lordofthejars.nosqlunit.mongodb;

import com.lordofthejars.nosqlunit.annotation.CustomComparisonStrategy;
import com.lordofthejars.nosqlunit.annotation.IgnorePropertyValue;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDb.InMemoryMongoRuleBuilder.newInMemoryMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbRule.MongoDbRuleBuilder.newMongoDbRule;

@CustomComparisonStrategy(comparisonStrategy = MongoFlexibleComparisonStrategy.class)
public class MongoFlexibleComparisonStrategyTest {

    @ClassRule
    public static final InMemoryMongoDb IN_MEMORY_MONGO_DB = newInMemoryMongoDbRule().build();

    @Rule
    public MongoDbRule mongoDbRule = newMongoDbRule().defaultManagedMongoDb("test");

    @Test
    @UsingDataSet(locations = "MongoFlexibleComparisonStrategyTest#thatShowWarnings.json")
    @ShouldMatchDataSet(location = "MongoFlexibleComparisonStrategyTest#thatShowWarnings-expected.json")
    @IgnorePropertyValue(properties = {"2", "collection.3"})
    public void thatShowsWarnings() {
    }
}
