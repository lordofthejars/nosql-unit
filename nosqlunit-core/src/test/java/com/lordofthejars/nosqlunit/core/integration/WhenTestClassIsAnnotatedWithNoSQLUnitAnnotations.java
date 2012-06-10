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
	public void annotated_class_without_locations_should_use_class_name_approach() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.REFRESH, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(MyTestClass.class, "my_unknown_test",  new ShouldMatchDataSetAnnotationTest());
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Default Class Name Strategy 2"});
		verify(databaseOperation, times(1)).databaseIs("Default Class Name Strategy 2");
		
	}
	
	
	@Test
	public void annotated_methods_without_locations_should_use_class_name_approach_if_method_file_not_found() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class, "my_unknown_test", new UsingDataSetAnnotationTest(LoadStrategyEnum.INSERT), new ShouldMatchDataSetAnnotationTest());
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Default Class Name Strategy"});
		verify(databaseOperation, times(1)).databaseIs("Default Class Name Strategy");
		
	}
	
	@Test
	public void annotated_methods_without_locations_should_use_method_name_approach() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class, "my_first_test", new UsingDataSetAnnotationTest(LoadStrategyEnum.INSERT), new ShouldMatchDataSetAnnotationTest());
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Default Method Name Strategy"});
		verify(databaseOperation, times(1)).databaseIs("Default Method Name Strategy");
		
	}
	
	
	@Test
	public void annotated_methods_should_have_precedence_over_annotated_class() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(DefaultClass.class, "WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations", new UsingDataSetAnnotationTest(new String[]{"test"}, LoadStrategyEnum.INSERT), new ShouldMatchDataSetAnnotationTest("test"));
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Method Annotation"});
		verify(databaseOperation, times(1)).databaseIs("Method Annotation");
		
	}
	
	
	@Test
	public void class_annotation_should_be_used_if_no_annotated_methods() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.REFRESH, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(DefaultClass.class, "WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations",  new ShouldMatchDataSetAnnotationTest("test"));
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Class Annotation"});
		verify(databaseOperation, times(1)).databaseIs("Method Annotation");
		
	}
	
	@Test
	public void class_annotation_should_be_used_if_any_annotated_methods() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.REFRESH, databaseOperation)).thenReturn(loadStrategyOperation);
		
		
		Description description = Description.createTestDescription(DefaultClass.class, "WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations");
		
		
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		verify(loadStrategyOperation, times(1)).executeScripts(new String[]{"Class Annotation"});
		verify(databaseOperation, times(1)).databaseIs("Class Annotation");
		
	}
	
}

@UsingDataSet(loadStrategy=LoadStrategyEnum.REFRESH)
class MyTestClass {
	
}

@UsingDataSet(loadStrategy=LoadStrategyEnum.REFRESH)
class MyUknownClass {
	
}

@UsingDataSet(locations="test2", loadStrategy=LoadStrategyEnum.REFRESH)
@ShouldMatchDataSet(location="test2")
class DefaultClass {
	
}
