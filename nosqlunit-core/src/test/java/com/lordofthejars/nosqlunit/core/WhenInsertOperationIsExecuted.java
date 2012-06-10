package com.lordofthejars.nosqlunit.core;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.anyString;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


public class WhenInsertOperationIsExecuted {

	@Mock private DatabaseOperation databaseOperation;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void insert_operations_should_be_executed() {
		
		InsertLoadStrategyOperation insertLoadStrategyOperation = new InsertLoadStrategyOperation(databaseOperation);
		String[] contents = new String[]{"My name is","Jimmy Pop"};
		
		insertLoadStrategyOperation.executeScripts(contents);
		verify(databaseOperation, times(2)).insert(anyString());
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void insert_operation_should_throw_an_exception_is_no_data_available() {
		
		InsertLoadStrategyOperation insertLoadStrategyOperation = new InsertLoadStrategyOperation(databaseOperation);
		String[] contents = new String[]{};
		insertLoadStrategyOperation.executeScripts(contents);
	}
	
}
