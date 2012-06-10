package com.lordofthejars.nosqlunit.core;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class WhenDeleteOperationIsExecuted {

	@Mock private DatabaseOperation databaseOperation;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void delete_operations_should_be_executed() {
		
		DeleteAllLoadStrategyOperation insertLoadStrategyOperation = new DeleteAllLoadStrategyOperation(databaseOperation);
		String[] contents = new String[]{"My name is","Jimmy Pop"};
		
		insertLoadStrategyOperation.executeScripts(contents);
		verify(databaseOperation, times(1)).deleteAll();
		
	}

	@Test
	public void delete_operations_should_be_called_executed_if_no_data_is_provided() {
		
		DeleteAllLoadStrategyOperation insertLoadStrategyOperation = new DeleteAllLoadStrategyOperation(databaseOperation);
		String[] contents = new String[]{};
		
		insertLoadStrategyOperation.executeScripts(contents);
		verify(databaseOperation, times(1)).deleteAll();
		
	}
	
}
