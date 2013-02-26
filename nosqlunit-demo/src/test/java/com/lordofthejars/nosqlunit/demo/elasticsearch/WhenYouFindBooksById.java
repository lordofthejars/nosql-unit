package com.lordofthejars.nosqlunit.demo.elasticsearch;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static com.lordofthejars.nosqlunit.elasticsearch.EmbeddedElasticsearch.EmbeddedElasticsearchRuleBuilder.newEmbeddedElasticsearchRule;
import static com.lordofthejars.nosqlunit.elasticsearch.ElasticsearchRule.ElasticsearchRuleBuilder.newElasticsearchRule;

import javax.inject.Inject;

import org.elasticsearch.client.Client;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.demo.model.Book;
import com.lordofthejars.nosqlunit.elasticsearch.ElasticsearchRule;
import com.lordofthejars.nosqlunit.elasticsearch.EmbeddedElasticsearch;

public class WhenYouFindBooksById {

	@ClassRule
	public static final EmbeddedElasticsearch EMBEDDED_ELASTICSEARCH = newEmbeddedElasticsearchRule().build();
	
	@Rule
	public ElasticsearchRule elasticsearchRule = newElasticsearchRule().defaultEmbeddedElasticsearch();
	
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
