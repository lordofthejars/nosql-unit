package com.lordofthejars.nosqlunit.demo.couchdb;

import static com.lordofthejars.nosqlunit.couchdb.CouchDbRule.CouchDbRuleBuilder.newCouchDbRule;
import static com.lordofthejars.nosqlunit.couchdb.ManagedCouchDb.ManagedCouchDbRuleBuilder.newManagedCouchDbRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.inject.Inject;

import org.ektorp.CouchDbConnector;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.couchdb.CouchDbRule;
import com.lordofthejars.nosqlunit.couchdb.ManagedCouchDb;
import com.lordofthejars.nosqlunit.demo.model.Book;

public class WhenYouFindBooksById {

	@ClassRule
	public static ManagedCouchDb managedCouchDb = newManagedCouchDbRule().couchDbPath("/usr/local").build();

	@Rule
	public CouchDbRule couchDbRule = newCouchDbRule().defaultManagedCouchDb("books");
	
	@Inject
	private CouchDbConnector couchDbConnector;
	
	@Test
	@UsingDataSet(locations="books.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void identified_book_should_be_returned() {
		
		BookManager bookManager = new BookManager(couchDbConnector);
		Book book = bookManager.findBookById("1");
		
		assertThat(book.getTitle(), is("The Hobbit"));
		assertThat(book.getNumberOfPages(), is(293));
		
	}
	
}
