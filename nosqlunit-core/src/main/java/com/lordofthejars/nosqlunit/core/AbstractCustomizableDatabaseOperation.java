package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

public abstract class AbstractCustomizableDatabaseOperation<S,T> implements DatabaseOperation<T> {

	protected InsertionStrategy<S> insertionStrategy;
	protected ComparisonStrategy<S> comparisonStrategy;
	
	public void setComparisonStrategy(ComparisonStrategy<S> comparisionStrategy) {
		this.comparisonStrategy = comparisionStrategy;
	}
	
	public boolean executeComparison(S connection, InputStream dataset) throws NoSqlAssertionError, Throwable {
		return comparisonStrategy.compare(connection, dataset);
	}
	
	public void setInsertionStrategy(InsertionStrategy<S> insertationStrategy) {
		this.insertionStrategy = insertationStrategy;
	}
	
	public void executeInsertion(S connection, InputStream dataset) throws Throwable {
		insertionStrategy.insert(connection, dataset);
	}
}
