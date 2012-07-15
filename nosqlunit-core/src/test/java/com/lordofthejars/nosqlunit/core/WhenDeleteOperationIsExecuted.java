package com.lordofthejars.nosqlunit.core;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

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
		InputStream[] contents = new InputStream[]{new ByteArrayInputStream("My name is".getBytes()), new ByteArrayInputStream("Jimmy Pop".getBytes())};
		
		insertLoadStrategyOperation.executeScripts(contents);
		verify(databaseOperation, times(1)).deleteAll();
		
	}

	@Test
	public void delete_operations_should_be_called_executed_if_no_data_is_provided() {
		
		DeleteAllLoadStrategyOperation insertLoadStrategyOperation = new DeleteAllLoadStrategyOperation(databaseOperation);
		InputStream[] contents = new InputStream[]{};
		
		insertLoadStrategyOperation.executeScripts(contents);
		verify(databaseOperation, times(1)).deleteAll();
		
	}
	
}
