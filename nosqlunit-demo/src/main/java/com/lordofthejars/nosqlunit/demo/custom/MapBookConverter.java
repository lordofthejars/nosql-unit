package com.lordofthejars.nosqlunit.demo.custom;

import java.util.HashMap;
import java.util.Map;

import com.lordofthejars.nosqlunit.demo.model.Book;

public class MapBookConverter {

	private static final String TITLE = "title";
	private static final String NUMBER_OF_PAGES = "numberOfPages"; 
	
	public static Book toBook(Map<String, Object> book) {
		return new Book((String)book.get(TITLE), Integer.parseInt((String) book.get(NUMBER_OF_PAGES)));
	}
	
	public static Map<String, Object> toMap(Book book) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(TITLE, book.getTitle());
		map.put(NUMBER_OF_PAGES, Integer.toString(book.getNumberOfPages()));
		return map;
	}
	
}
