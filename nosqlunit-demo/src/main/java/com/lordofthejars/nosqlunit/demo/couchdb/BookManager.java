package com.lordofthejars.nosqlunit.demo.couchdb;

import java.util.Map;

import org.ektorp.CouchDbConnector;

import com.lordofthejars.nosqlunit.demo.model.Book;

public class BookManager {

	private CouchDbConnector connector;
	
	public BookManager(CouchDbConnector connector)  {
		this.connector = connector;
	}
	
	public void create(Book book) {
		connector.create(MapBookConverter.toMap(book));
	}

	public Book findBookById(String id) {
		Map<String, Object> map = connector.get(Map.class, id);
		return MapBookConverter.toBook(map);
	}
	
}
