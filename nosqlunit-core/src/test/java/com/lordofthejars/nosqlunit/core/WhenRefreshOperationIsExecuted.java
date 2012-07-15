package com.lordofthejars.nosqlunit.core;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class WhenRefreshOperationIsExecuted {

	@Mock private DatabaseOperation databaseOperation;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void insert_operations_should_be_executed() {
		
		RefreshLoadStrategyOperation refreshLoadStrategyOperation = new RefreshLoadStrategyOperation(databaseOperation);
		InputStream[] contents = new InputStream[]{new ByteArrayInputStream("My name is".getBytes()), new ByteArrayInputStream("Jimmy Pop".getBytes())};
		
		refreshLoadStrategyOperation.executeScripts(contents);
		verify(databaseOperation, times(2)).insertNotPresent(any(InputStream.class));
		
	}

	@Test(expected=IllegalArgumentException.class)
	public void refresh_operation_should_throw_an_exception_is_no_data_available() {
		
		RefreshLoadStrategyOperation refreshLoadStrategyOperation = new RefreshLoadStrategyOperation(databaseOperation);
		InputStream[] contents = new InputStream[]{};
		refreshLoadStrategyOperation.executeScripts(contents);
	}
	
}
