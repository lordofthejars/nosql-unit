package com.lordofthejars.nosqlunit.core;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

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
		InputStream[] contents = new InputStream[]{new ByteArrayInputStream("My name is".getBytes()), new ByteArrayInputStream("Jimmy Pop".getBytes())};
		
		cleanInsertLoadStrategyOperation.executeScripts(contents);
		verify(databaseOperation, times(1)).deleteAll();
		verify(databaseOperation, times(2)).insert(any(InputStream.class));
		
	}
	
}
