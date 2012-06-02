package com.lordofthejars.nosqlunit.core.integration;


import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.core.LoadStrategyFactory;
import com.lordofthejars.nosqlunit.core.LoadStrategyOperation;
import com.lordofthejars.nosqlunit.core.ShouldMatchDataSetAnnotationTest;
import com.lordofthejars.nosqlunit.core.UsingDataSetAnnotationTest;


@UsingDataSet(locations="test2", loadStrategy=LoadStrategyEnum.REFRESH)
@ShouldMatchDataSet(values="test2")
public class WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations {

	@Mock
	public Statement base;
	
	@Mock
	public LoadStrategyFactory loadStrategyFactory;
	
	@Mock
	public DatabaseOperation databaseOperation;
	
	@Mock
	public LoadStrategyOperation loadStrategyOperation;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void annotated_methods_should_have_precedence_over_annotated_class() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class, "WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations", new UsingDataSetAnnotationTest(new String[]{"test"}, LoadStrategyEnum.INSERT), new ShouldMatchDataSetAnnotationTest(new String[]{"test"}));
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setResourceBase(WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Method Annotation"});
		verify(databaseOperation, times(1)).nonStrictAssertEquals("Method Annotation");
		
	}
	
	
	@Test
	public void class_annotation_should_be_used_if_no_annotated_methods() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.REFRESH, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class, "WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations",  new ShouldMatchDataSetAnnotationTest(new String[]{"test"}));
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setResourceBase(WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Class Annotation"});
		verify(databaseOperation, times(1)).nonStrictAssertEquals("Method Annotation");
		
	}
	
	@Test
	public void class_annotation_should_be_used_if_any_annotated_methods() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.REFRESH, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class, "WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations");
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setResourceBase(WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Class Annotation"});
		verify(databaseOperation, times(1)).nonStrictAssertEquals("Class Annotation");
		
	}
	
}
