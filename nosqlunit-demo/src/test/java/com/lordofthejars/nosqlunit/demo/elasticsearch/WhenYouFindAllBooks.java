package com.lordofthejars.nosqlunit.demo.elasticsearch;


import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.demo.model.Book;
import com.lordofthejars.nosqlunit.elasticsearch2.ElasticsearchRule;
import com.lordofthejars.nosqlunit.elasticsearch2.ManagedElasticsearch;
import org.elasticsearch.client.Client;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Inject;
import java.util.List;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

public class WhenYouFindAllBooks {

	@ClassRule
	public static final ManagedElasticsearch MANAGED_EALSTICSEARCH = ManagedElasticsearch.ManagedElasticsearchRuleBuilder.newManagedElasticsearchRule().elasticsearchPath("/opt/elasticsearch-0.20.5").build();
	
	@Rule
	public ElasticsearchRule elasticsearchRule = ElasticsearchRule.ElasticsearchRuleBuilder.newElasticsearchRule().defaultManagedElasticsearch();
	
	@Inject
	private Client client;
	
	@Test
	@UsingDataSet(locations="books.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void all_books_should_be_returned() {
		
		BookManager bookManager = new BookManager(client);
		List<Book> books = bookManager.searchAllBooks();
		
		assertThat(books, hasItems(new Book("The Hobbit", 293)));
		
	}
	
}
