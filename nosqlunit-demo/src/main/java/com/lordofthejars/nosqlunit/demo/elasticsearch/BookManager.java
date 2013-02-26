package com.lordofthejars.nosqlunit.demo.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import com.lordofthejars.nosqlunit.demo.custom.MapBookConverter;
import com.lordofthejars.nosqlunit.demo.model.Book;

public class BookManager {

	private Client client;

	public BookManager(Client client) {
		this.client = client;
	}

	public void create(Book book) {
		IndexRequestBuilder indexRequestBuilder = this.client.prepareIndex("books", "book", null).setSource(
				MapBookConverter.toMap(book));
		indexRequestBuilder.execute().actionGet();
	}

	public Book findBookById(String id) {
		GetResponse actionGet = this.client.prepareGet("books", "book", id).execute().actionGet();
		return MapBookConverter.toBook(actionGet.getSource());
	}

	public List<Book> searchAllBooks() {
		
		List<Book> books = new ArrayList<Book>();
		
		SearchResponse response = this.client.prepareSearch().execute().actionGet();
		
		for (SearchHit hit : response.getHits()) {
			books.add(MapBookConverter.toBook(hit.getSource()));
		}
		
		return books;
		
	}
	
}
