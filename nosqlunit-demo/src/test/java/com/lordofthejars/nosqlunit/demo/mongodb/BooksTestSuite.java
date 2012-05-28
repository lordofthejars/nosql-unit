package com.lordofthejars.nosqlunit.demo.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;

import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;

@RunWith(Suite.class)
@SuiteClasses({WhenANewBookIsCreated.class, WhenYouFindAllBooks.class})
public class BooksTestSuite {

	@ClassRule
	public static ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo").appendSingleCommandLineArguments("-vvv")
	.build();
	
}
