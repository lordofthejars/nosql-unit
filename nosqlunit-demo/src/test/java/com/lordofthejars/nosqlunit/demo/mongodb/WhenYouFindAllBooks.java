package com.lordofthejars.nosqlunit.demo.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder.mongoDb;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.DataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.demo.model.Book;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;

public class WhenYouFindAllBooks {


	@Rule
	public MongoDbRule remoteMongoDbRule = new MongoDbRule(WhenYouFindAllBooks.class,  mongoDb()
			.databaseName("test").build());
	
	@Test
	@DataSet(locations="initialData.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void manager_should_return_all_inserted_books() {
		
		BookManager bookManager = new BookManager(MongoDbUtil.getCollection(Book.class.getSimpleName()));
		List<Book> books = bookManager.findAll();
		
		Book expectedBook = new Book("The Hobbit", 293);
		
		assertThat(books, hasSize(1));
		assertThat(expectedBook, isIn(books));
		
	}
	
	@Test
	@DataSet(loadStrategy=LoadStrategyEnum.DELETE_ALL)
	public void manager_should_return_empty_list_when_no_books() {
		
		BookManager bookManager = new BookManager(MongoDbUtil.getCollection(Book.class.getSimpleName()));
		List<Book> books = bookManager.findAll();
		
		assertThat(books, hasSize(0));
		
	}
	
	
}
