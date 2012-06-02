package com.lordofthejars.nosqlunit.core.integration;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.beans.Statement;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Description;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.core.UsingDataSetAnnotationTest;


public class WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations {

	@Mock
	public Statement base;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void annotated_methods_should_have_precedence_over_annotated_class() {
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		
		Description description = mock(Description.class);
		when(description.getAnnotation(UsingDataSet.class)).thenReturn(new UsingDataSetAnnotationTest(new String[]{"test"}, LoadStrategyEnum.INSERT));
		
		
		
	}
	
	
}
