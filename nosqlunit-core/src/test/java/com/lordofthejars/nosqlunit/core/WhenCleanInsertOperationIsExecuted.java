package com.lordofthejars.nosqlunit.core;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.anyString;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


public class WhenCleanInsertOperationIsExecuted {

	@Mock private DatabaseOperation databaseOperation;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void clean_and_insert_operations_should_be_executed() {
		
		CleanInsertLoadStrategyOperation cleanInsertLoadStrategyOperation = new CleanInsertLoadStrategyOperation(databaseOperation);
		String[] contents = new String[]{"My name is","Jimmy Pop"};
		
		cleanInsertLoadStrategyOperation.executeScripts(contents);
		verify(databaseOperation, times(1)).deleteAll();
		verify(databaseOperation, times(2)).insert(anyString());
		
	}

	@Test(expected=IllegalArgumentException.class)
	public void clean_and_insert_operation_should_throw_an_exception_is_no_data_available() {
		
		CleanInsertLoadStrategyOperation cleanInsertLoadStrategyOperation = new CleanInsertLoadStrategyOperation(databaseOperation);
		String[] contents = new String[]{};
		cleanInsertLoadStrategyOperation.executeScripts(contents);
	}
	
}
