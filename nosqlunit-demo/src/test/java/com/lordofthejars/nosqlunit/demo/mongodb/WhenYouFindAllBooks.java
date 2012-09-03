package com.lordofthejars.nosqlunit.demo.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbRule.MongoDbRuleBuilder.newMongoDbRule;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.demo.model.Book;
import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;

@UsingDataSet
public class WhenYouFindAllBooks {


	@ClassRule
	public static ManagedMongoDb managedMongoDb = newManagedMongoDbRule().mongodPath("/opt/mongo").appendSingleCommandLineArguments("-vvv")
	.build();
	
	@Rule
	public MongoDbRule remoteMongoDbRule = newMongoDbRule().defaultManagedMongoDb("test");
	
	@Test
	@UsingDataSet(locations="initialData.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void manager_should_return_all_inserted_books() {
		
		BookManager bookManager = new BookManager(MongoDbUtil.getCollection(Book.class.getSimpleName()));
		List<Book> books = bookManager.findAll();
		
		Book expectedBook = new Book("The Hobbit", 293);
		
		assertThat(books, hasSize(1));
		assertThat(expectedBook, isIn(books));
		
	}
	
	@Test
	@UsingDataSet(loadStrategy=LoadStrategyEnum.DELETE_ALL)
	public void manager_should_return_empty_list_when_no_books() {
		
		BookManager bookManager = new BookManager(MongoDbUtil.getCollection(Book.class.getSimpleName()));
		List<Book> books = bookManager.findAll();
		
		assertThat(books, hasSize(0));
		
	}
	
	
}
