package com.lordofthejars.nosqlunit.demo.mongodb;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.demo.model.Book;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class BookManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(BookManager.class);
	
	private static final MongoDbBookConverter MONGO_DB_BOOK_CONVERTER = new MongoDbBookConverter();
	private static final DbObjectBookConverter DB_OBJECT_BOOK_CONVERTER = new DbObjectBookConverter();
	
	private DBCollection booksCollection;
	
	public BookManager(DBCollection booksCollection) {
		this.booksCollection = booksCollection;
	}
	
	public void create(Book book) {
		DBObject dbObject = MONGO_DB_BOOK_CONVERTER.convert(book);
		booksCollection.insert(dbObject);
	}
	
	public List<Book> findAll() {
		
		LOGGER.debug("Finding All Elements of Collection "+booksCollection.getName());
		
		List<Book> books = new ArrayList<Book>();
		DBCursor findAll = booksCollection.find();
		
		while(findAll.hasNext()) {
			books.add(DB_OBJECT_BOOK_CONVERTER.convert(findAll.next()));
		}
		
		return books;
	}
	
}
