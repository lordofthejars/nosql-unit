package com.lordofthejars.nosqlunit.core.integration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;

import java.io.InputStream;
import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.annotation.CustomComparisonStrategy;
import com.lordofthejars.nosqlunit.annotation.CustomInsertionStrategy;
import com.lordofthejars.nosqlunit.annotation.Selective;
import com.lordofthejars.nosqlunit.annotation.SelectiveMatcher;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.ComparisonStrategy;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.core.IOUtils;
import com.lordofthejars.nosqlunit.core.InjectAnnotationProcessor;
import com.lordofthejars.nosqlunit.core.InsertionStrategy;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.core.LoadStrategyFactory;
import com.lordofthejars.nosqlunit.core.LoadStrategyOperation;

public class WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations {

	@Mock
	public Statement base;

	@Mock
	public LoadStrategyFactory loadStrategyFactory;

	@Mock
	public DatabaseOperation databaseOperation;

	@Mock
	public AbstractCustomizableDatabaseOperation abstractCustomizableDatabaseOperation;
	
	@Mock
	public LoadStrategyOperation loadStrategyOperation;

	@Mock
	public InjectAnnotationProcessor injectAnnotationProcessor;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void selective_annotations_should_not_load_data_of_not_identified_rules_but_global() throws Throwable {
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(MyGlobalAndSelectiveClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		abstractNoSqlTestRule.setIdentifier("two");

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MyGlobalAndSelectiveClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamCaptor = ArgumentCaptor.forClass(InputStream[].class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamCaptor.capture());

		InputStream[] isContents = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Class Annotation"));

	}

	@Test
	public void selective_annotations_should_load_only_load_data_of_identified_rules_and_global_data() throws Throwable {
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(MyGlobalAndSelectiveClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		abstractNoSqlTestRule.setIdentifier("one");

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MyGlobalAndSelectiveClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamCaptor = ArgumentCaptor.forClass(InputStream[].class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamCaptor.capture());

		InputStream[] isContents = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Class Annotation"));

		scriptContent = IOUtils.readFullStream(isContents[1]);
		assertThat(scriptContent, is("Selective Annotation"));
	}

	@Test
	public void selective_annotations_should_not_load_data_of_not_identified_rules() throws Throwable {
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(MySelectiveClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		abstractNoSqlTestRule.setIdentifier("two");

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		ArgumentCaptor<InputStream[]> streamCaptor = ArgumentCaptor.forClass(InputStream[].class);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MySelectiveClass()).evaluate();

		verify(loadStrategyOperation, times(1)).executeScripts(streamCaptor.capture());

		assertThat(streamCaptor.getValue(), arrayWithSize(0));

	}

	@Test
	public void selective_annotations_should_load_only_load_data_of_identified_rules() throws Throwable {
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(MySelectiveClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		abstractNoSqlTestRule.setIdentifier("one");

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MySelectiveClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamCaptor = ArgumentCaptor.forClass(InputStream[].class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamCaptor.capture());

		InputStream[] isContents = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Selective Annotation"));

	}

	
	
	@Test
	public void annotated_class_without_locations_should_use_class_name_approach() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(MyTestClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MyTestClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamsCaptor.capture());

		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Default Class Name Strategy 2"));

		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Default Class Name Strategy 2"));

	}

	@Test
	public void annotated_methods_without_locations_should_use_class_name_approach_if_method_file_not_found()
			throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(MyTestMethodClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MyTestMethodClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamsCaptor.capture());

		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Default Class Name Strategy"));

		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Default Class Name Strategy"));

	}

	@Test
	public void customized_comparision_test_classes_should_insert_data_using_customized_approach() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, abstractCustomizableDatabaseOperation)).thenReturn(
				loadStrategyOperation);
		
		FrameworkMethod frameworkMethod = frameworkMethod(MyTestWithCustomComparisionStrategy.class, "my_unknown_test");
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(abstractCustomizableDatabaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(abstractCustomizableDatabaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MyTestWithCustomComparisionStrategy()).evaluate();
		
		verify(abstractCustomizableDatabaseOperation, times(1)).setComparisonStrategy(any(ComparisonStrategy.class));
		
	}
	
	@Test
	public void customized_insertation_test_classes_should_insert_data_using_customized_approach() throws Throwable {
		
		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, abstractCustomizableDatabaseOperation)).thenReturn(
				loadStrategyOperation);
		
		FrameworkMethod frameworkMethod = frameworkMethod(MyTestWithCustomInsertStrategy.class, "my_unknown_test");
		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(abstractCustomizableDatabaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(abstractCustomizableDatabaseOperation);
		
		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MyTestWithCustomInsertStrategy()).evaluate();
		
		verify(abstractCustomizableDatabaseOperation, times(1)).setInsertionStrategy(any(InsertionStrategy.class));
		
	}
	
	@Test
	public void annotated_methods_without_locations_should_use_method_name_approach() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(MyTestMethodClass.class, "my_method_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new MyTestMethodClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamsCaptor.capture());

		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Default Method Name Strategy"));

		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Default Method Name Strategy"));

	}

	@Test
	public void annotated_methods_should_have_precedence_over_annotated_class() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(DefaultClass.class, "my_method_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new DefaultClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamsCaptor.capture());

		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Method Annotation"));

		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Method Annotation"));

	}

	@Test
	public void class_annotation_should_be_used_if_no_annotated_methods() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(DefaultClass.class, "my_unknown_test_2");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new DefaultClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamsCaptor.capture());

		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Class Annotation"));

		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Method Annotation"));

	}

	@Test
	public void class_annotation_should_be_used_if_any_annotated_methods() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(DefaultClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new DefaultClass()).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(streamsCaptor.capture());

		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Class Annotation"));

		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Class Annotation"));

	}

	@Test
	public void selective_matchers_annotation_should_only_verify_identified_connection() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(SelectiveDefaultClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		abstractNoSqlTestRule.setIdentifier("one");

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new SelectiveDefaultClass()).evaluate();

		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Selective Annotation"));

	}

	@Test(expected = IllegalArgumentException.class)
	public void selective_matchers_annotation_should_fail_if_unknown_identified_connection() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(SelectiveDefaultClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		abstractNoSqlTestRule.setIdentifier("two");

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new SelectiveDefaultClass()).evaluate();
		fail();

	}

	@Test
	public void global_location_should_have_precedence_over_selective_matchers() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(SelectiveAndLocationClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		abstractNoSqlTestRule.setIdentifier("one");

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, frameworkMethod, new SelectiveAndLocationClass()).evaluate();

		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Class Annotation"));

	}

	@Test
	public void not_valid_locations_should_throw_an_exception() throws Throwable {

		when(loadStrategyFactory.getLoadStrategyInstance(LoadStrategyEnum.INSERT, databaseOperation)).thenReturn(
				loadStrategyOperation);

		FrameworkMethod frameworkMethod = frameworkMethod(MyUknownClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule).getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule.setInjectAnnotationProcessor(injectAnnotationProcessor);

		try {
			abstractNoSqlTestRule.apply(base, frameworkMethod, new MyUknownClass()).evaluate();
			fail();
		} catch (IllegalArgumentException e) {
			assertThat(
					e.getMessage(),
					is("File specified in locations property are not present in classpath, or no files matching default name are found. Valid default locations are: /com/lordofthejars/nosqlunit/core/integration/MyUknownClass.json or /com/lordofthejars/nosqlunit/core/integration/MyUknownClass#my_unknown_test.json"));
		}

	}

	private FrameworkMethod frameworkMethod(Class<?> testClass, String methodName) {

		try {
			Method method = testClass.getMethod(methodName);
			return new FrameworkMethod(method);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		}

	}

}

@UsingDataSet(locations = "test2", loadStrategy=LoadStrategyEnum.INSERT)
@ShouldMatchDataSet(location = "test2")
@CustomComparisonStrategy(comparisonStrategy=MyCustomComparision.class)
class MyTestWithCustomComparisionStrategy {
	
	@Test
	public void my_unknown_test() {
		
	}
}

@UsingDataSet(locations = "test2", loadStrategy=LoadStrategyEnum.INSERT)
@CustomInsertionStrategy(insertionStrategy=MyCustomInsertation.class)
class MyTestWithCustomInsertStrategy {
	
	@Test
	public void my_unknown_test() {
		
	}
}

@UsingDataSet(locations = "test2", withSelectiveLocations = { @Selective(identifier = "one", locations = "test3") }, loadStrategy = LoadStrategyEnum.INSERT)
class MyGlobalAndSelectiveClass {

	@Test
	public void my_unknown_test() {
	}

}

@UsingDataSet(withSelectiveLocations = { @Selective(identifier = "one", locations = "test3") }, loadStrategy = LoadStrategyEnum.INSERT)
class MySelectiveClass {

	@Test
	public void my_unknown_test() {
	}

}

@UsingDataSet(loadStrategy = LoadStrategyEnum.INSERT)
class MyTestClass {

	@Test
	@ShouldMatchDataSet()
	public void my_unknown_test() {
	}

}

class MyTestMethodClass {
	@UsingDataSet(loadStrategy = LoadStrategyEnum.INSERT)
	@Test
	@ShouldMatchDataSet()
	public void my_unknown_test() {
	}

	@UsingDataSet(loadStrategy = LoadStrategyEnum.INSERT)
	@Test
	@ShouldMatchDataSet()
	public void my_method_test() {
	}
}

@UsingDataSet(loadStrategy = LoadStrategyEnum.INSERT)
class MyUknownClass {

	@Test
	public void my_unknown_test() {
	}

}

@UsingDataSet(locations = "test2", loadStrategy = LoadStrategyEnum.INSERT)
@ShouldMatchDataSet(location = "test2")
class DefaultClass {

	@Test
	public void my_unknown_test() {
	}

	@Test
	@ShouldMatchDataSet(location = "test")
	public void my_unknown_test_2() {
	}

	@Test
	@UsingDataSet(locations = "test", loadStrategy = LoadStrategyEnum.INSERT)
	@ShouldMatchDataSet(location = "test")
	public void my_method_test() {
	}

}

@ShouldMatchDataSet(withSelectiveMatcher = { @SelectiveMatcher(identifier = "one", location = "test3") })
class SelectiveDefaultClass {

	@Test
	public void my_unknown_test() {
	}

}

@ShouldMatchDataSet(location = "test2", withSelectiveMatcher = { @SelectiveMatcher(identifier = "one", location = "test3") })
class SelectiveAndLocationClass {

	@Test
	public void my_unknown_test() {
	}

}
