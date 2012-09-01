package com.lordofthejars.nosqlunit.demo.redis;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

import com.lordofthejars.nosqlunit.demo.model.Book;


public class BookManager {
	
	private static final String TITLE_FIELD_NAME = "title";
	private static final String NUMBER_OF_PAGES = "numberOfPages";
	
	private Jedis jedis;
	
	public BookManager(Jedis jedis) {
		this.jedis = jedis;
	}
	
	public void insertBook(Book book) {
		
		Map<String, String> fields = new HashMap<String, String>();
		
		fields.put(TITLE_FIELD_NAME, book.getTitle());
		fields.put(NUMBER_OF_PAGES, Integer.toString(book.getNumberOfPages()));
		
		jedis.hmset(book.getTitle(), fields);
	}

	public Book findBookByTitle(String title) {
		
		Map<String, String> fields = jedis.hgetAll(title);
		return new Book(fields.get(TITLE_FIELD_NAME), Integer.parseInt(fields.get(NUMBER_OF_PAGES)));
		
	}
	
}
