package com.lordofthejars.nosqlunit.demo.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManagerBuilder.newManagedMongoDbLifecycle;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbRule.MongoDbRuleBuilder.newMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.ReplicationMongoDbConfigurationBuilder.replicationMongoDbConfiguration;
import static com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetBuilder.replicaSet;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.Assert.assertThat;

import java.util.List;

import javax.inject.Inject;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.demo.model.Book;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetManagedMongoDb;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class WhenYouFindAllBooksInReplicaSet {

	static {
		System.setProperty("MONGO_HOME", "/opt/mongo");
	}

	@ClassRule
	public static ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet(
			"rs-test")
			.eligible(
					newManagedMongoDbLifecycle().port(27017)
							.dbRelativePath("rs-0").logRelativePath("log-0")
							.get())
			.eligible(
					newManagedMongoDbLifecycle().port(27018)
							.dbRelativePath("rs-1").logRelativePath("log-1")
							.get())
			.arbiter(
					newManagedMongoDbLifecycle().port(27019)
							.dbRelativePath("rs-2").logRelativePath("log-2")
							.get())
			.get();

	@Rule
	public MongoDbRule mongoDbRule = newMongoDbRule().configure(
			replicationMongoDbConfiguration().databaseName("test")
											 .seed("localhost", 27017)
											 .seed("localhost", 27018)
											 .configure())
										.build();

	@Inject
	private MongoClient mongo;

	@Test
	@UsingDataSet(locations = "initialData.json", loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	public void manager_should_return_all_inserted_books() {

		BookManager bookManager = new BookManager(bookCollection());
		List<Book> books = bookManager.findAll();

		Book expectedBook = new Book("The Hobbit", 293);

		assertThat(books, hasSize(1));
		assertThat(expectedBook, isIn(books));

	}

	private DBCollection bookCollection() {
		return mongo.getDB("test").getCollection(Book.class.getSimpleName());
	}

}
