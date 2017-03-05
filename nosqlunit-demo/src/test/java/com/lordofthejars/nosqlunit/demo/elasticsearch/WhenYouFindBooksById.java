package com.lordofthejars.nosqlunit.demo.elasticsearch;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.demo.model.Book;
import com.lordofthejars.nosqlunit.elasticsearch2.ElasticsearchRule;
import com.lordofthejars.nosqlunit.elasticsearch2.EmbeddedElasticsearch;
import org.elasticsearch.client.Client;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Inject;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WhenYouFindBooksById {

	@ClassRule
	public static final EmbeddedElasticsearch EMBEDDED_ELASTICSEARCH = EmbeddedElasticsearch.EmbeddedElasticsearchRuleBuilder.newEmbeddedElasticsearchRule().build();
	
	@Rule
	public ElasticsearchRule elasticsearchRule = ElasticsearchRule.ElasticsearchRuleBuilder.newElasticsearchRule().defaultEmbeddedElasticsearch();
	
	@Inject
	private Client client;
	
	@Test
	@UsingDataSet(locations="books.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void books_with_properties_should_be_returned() {
		
		BookManager bookManager = new BookManager(client);
		Book book = bookManager.findBookById("1");
		
		assertThat(book.getTitle(), is("The Hobbit"));
		
	}
}
