package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

public abstract class AbstractCustomizableDatabaseOperation<S,T> implements DatabaseOperation<T> {

	protected InsertationStrategy<S> insertationStrategy;
	protected ComparisionStrategy<S> comparisionStrategy;
	
	public void setComparisionStrategy(ComparisionStrategy<S> comparisionStrategy) {
		this.comparisionStrategy = comparisionStrategy;
	}
	
	public boolean executeComparision(S connection, InputStream dataset) throws NoSqlAssertionError, Throwable {
		return comparisionStrategy.compare(connection, dataset);
	}
	
	public void setInsertationStrategy(InsertationStrategy<S> insertationStrategy) {
		this.insertationStrategy = insertationStrategy;
	}
	
	public void executeInsertation(S connection, InputStream dataset) throws Throwable {
		insertationStrategy.insert(connection, dataset);
	}
}
