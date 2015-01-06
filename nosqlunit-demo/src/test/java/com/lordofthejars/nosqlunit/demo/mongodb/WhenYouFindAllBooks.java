package com.lordofthejars.nosqlunit.demo.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDb.InMemoryMongoRuleBuilder.newInMemoryMongoDbRule;
import static com.lordofthejars.nosqlunit.mongodb.MongoDbRule.MongoDbRuleBuilder.newMongoDbRule;
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
import com.lordofthejars.nosqlunit.mongodb.InMemoryMongoDb;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

@UsingDataSet(locations="initialData.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
public class WhenYouFindAllBooks {


	@ClassRule
	public static final InMemoryMongoDb IN_MEMORY_MONGO_DB = newInMemoryMongoDbRule().build();
	
	@Rule
	public MongoDbRule remoteMongoDbRule = newMongoDbRule().defaultEmbeddedMongoDb("test");
	
	@Inject
	private Mongo mongo;
	
	@Test
	public void manager_should_return_all_inserted_books() {
		
		BookManager bookManager = new BookManager(bookCollection());
		List<Book> books = bookManager.findAll();
		
		Book expectedBook = new Book("The Hobbit", 293);
		
		assertThat(books, hasSize(1));
		assertThat(expectedBook, isIn(books));
		
	}
	
	@Test
	@UsingDataSet(loadStrategy=LoadStrategyEnum.DELETE_ALL)
	public void manager_should_return_empty_list_when_no_books() {
		
		BookManager bookManager = new BookManager(bookCollection());
		List<Book> books = bookManager.findAll();
		
		assertThat(books, hasSize(0));
		
	}
	
	private DBCollection bookCollection() {
		return mongo.getDB("test").getCollection(Book.class.getSimpleName());
	}
}
