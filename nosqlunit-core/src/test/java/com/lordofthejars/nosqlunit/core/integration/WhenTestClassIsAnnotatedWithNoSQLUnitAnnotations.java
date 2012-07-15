package com.lordofthejars.nosqlunit.core.integration;

import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.hamcrest.CoreMatchers.is;

import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.annotation.Selective;
import com.lordofthejars.nosqlunit.annotation.SelectiveMatcher;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.core.IOUtils;
import com.lordofthejars.nosqlunit.core.InjectAnnotationProcessor;
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

	@Mock
	public InjectAnnotationProcessor injectAnnotationProcessor;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void selective_annotations_should_load_not_load_data_of_not_identified_rules_but_global()
			throws Throwable {
		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				MyGlobalAndSelectiveClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		abstractNoSqlTestRule.setIdentifier("two");

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream[]> streamCaptor = ArgumentCaptor.forClass(InputStream[].class);
		
		verify(loadStrategyOperation, times(1)).executeScripts(
				streamCaptor.capture());
		
		InputStream[] isContents = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Class Annotation"));
		
	}

	@Test
	public void selective_annotations_should_load_only_load_data_of_identified_rules_and_global_data()
			throws Throwable {
		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				MyGlobalAndSelectiveClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		abstractNoSqlTestRule.setIdentifier("one");

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream[]> streamCaptor = ArgumentCaptor.forClass(InputStream[].class);
	

		verify(loadStrategyOperation, times(1)).executeScripts(
				streamCaptor.capture());
		
		InputStream[] isContents = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Class Annotation"));
		
		scriptContent = IOUtils.readFullStream(isContents[1]);
		assertThat(scriptContent, is("Selective Annotation"));
	}

	@Test
	public void selective_annotations_should_not_load_data_of_not_identified_rules()
			throws Throwable {
		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				MySelectiveClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		abstractNoSqlTestRule.setIdentifier("two");

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		ArgumentCaptor<InputStream[]> streamCaptor = ArgumentCaptor.forClass(InputStream[].class);
		
		abstractNoSqlTestRule.apply(base, description).evaluate();

		verify(loadStrategyOperation, times(1)).executeScripts(
				streamCaptor.capture());
		
		assertThat(streamCaptor.getValue(), arrayWithSize(0));
				
	}

	@Test
	public void selective_annotations_should_load_only_load_data_of_identified_rules()
			throws Throwable {
		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				MySelectiveClass.class, "my_unknown_test");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);
		abstractNoSqlTestRule.setIdentifier("one");

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		
		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream[]> streamCaptor = ArgumentCaptor.forClass(InputStream[].class);
		
		
		verify(loadStrategyOperation, times(1)).executeScripts(
				streamCaptor.capture());
		
		InputStream[] isContents = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Selective Annotation"));
		
		
	}

	@Test
	public void annotated_class_without_locations_should_use_class_name_approach()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				MyTestClass.class, "my_unknown_test",
				new ShouldMatchDataSetAnnotationTest());

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
		
		verify(loadStrategyOperation, times(1)).executeScripts(
				streamsCaptor.capture());
		
		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Default Class Name Strategy 2"));
		
		
		
		verify(databaseOperation, times(1)).databaseIs(
				streamCaptor.capture());
		
		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Default Class Name Strategy 2"));
		
	}

	@Test
	public void annotated_methods_without_locations_should_use_class_name_approach_if_method_file_not_found()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.INSERT, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class,
				"my_unknown_test", new UsingDataSetAnnotationTest(
						LoadStrategyEnum.INSERT),
				new ShouldMatchDataSetAnnotationTest());

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(
				streamsCaptor.capture());
		
		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Default Class Name Strategy"));
		
		verify(databaseOperation, times(1)).databaseIs(
				streamCaptor.capture());
		
		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Default Class Name Strategy"));

	}

	@Test
	public void annotated_methods_without_locations_should_use_method_name_approach()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.INSERT, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations.class,
				"my_first_test", new UsingDataSetAnnotationTest(
						LoadStrategyEnum.INSERT),
				new ShouldMatchDataSetAnnotationTest());

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(
				streamsCaptor.capture());
		
		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Default Method Name Strategy"));
		
		verify(databaseOperation, times(1)).databaseIs(
				streamCaptor.capture());
		
		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Default Method Name Strategy"));
		
	}

	@Test
	public void annotated_methods_should_have_precedence_over_annotated_class()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.INSERT, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				DefaultClass.class,
				"WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations",
				new UsingDataSetAnnotationTest(new String[] { "test" },
						LoadStrategyEnum.INSERT),
				new ShouldMatchDataSetAnnotationTest("test"));

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		
		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(
				streamsCaptor.capture());
		
		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Method Annotation"));
		
		verify(databaseOperation, times(1)).databaseIs(
				streamCaptor.capture());
		
		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Method Annotation"));
		

	}

	@Test
	public void class_annotation_should_be_used_if_no_annotated_methods()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				DefaultClass.class,
				"WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations",
				new ShouldMatchDataSetAnnotationTest("test"));

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(
				streamsCaptor.capture());
		
		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Class Annotation"));
		
		verify(databaseOperation, times(1)).databaseIs(
				streamCaptor.capture());
		
		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Method Annotation"));
		
	}

	@Test
	public void class_annotation_should_be_used_if_any_annotated_methods()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				DefaultClass.class,
				"WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();
		
		ArgumentCaptor<InputStream[]> streamsCaptor = ArgumentCaptor.forClass(InputStream[].class);
		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);

		verify(loadStrategyOperation, times(1)).executeScripts(
				streamsCaptor.capture());
		
		InputStream[] isContents = streamsCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContents[0]);
		assertThat(scriptContent, is("Class Annotation"));
		
		verify(databaseOperation, times(1)).databaseIs(
				streamCaptor.capture());
		
		InputStream isContent = streamCaptor.getValue();
		scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Class Annotation"));
		

	}

	@Test
	public void selective_matchers_annotation_should_only_verify_identified_connection()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				SelectiveDefaultClass.class,
				"WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		abstractNoSqlTestRule.setIdentifier("one");

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
		
		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());
		
		InputStream isContent = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Selective Annotation"));
		

	}

	@Test(expected = IllegalArgumentException.class)
	public void selective_matchers_annotation_should_fail_if_unknown_identified_connection()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				SelectiveDefaultClass.class,
				"WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn("json").when(abstractNoSqlTestRule).getWorkingExtension();
		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		abstractNoSqlTestRule.setIdentifier("two");

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

	}

	@Test
	public void global_location_should_have_precedence_over_selective_matchers()
			throws Throwable {

		when(
				loadStrategyFactory.getLoadStrategyInstance(
						LoadStrategyEnum.REFRESH, databaseOperation))
				.thenReturn(loadStrategyOperation);

		Description description = Description.createTestDescription(
				SelectiveAndLocationClass.class,
				"WhenTestClassIsAnnotatedWithNoSQLUnitAnnotations");

		AbstractNoSqlTestRule abstractNoSqlTestRule = mock(
				AbstractNoSqlTestRule.class, Mockito.CALLS_REAL_METHODS);

		doReturn(databaseOperation).when(abstractNoSqlTestRule)
				.getDatabaseOperation();
		abstractNoSqlTestRule.setIdentifier("one");

		when(abstractNoSqlTestRule.getDatabaseOperation()).thenReturn(
				databaseOperation);

		abstractNoSqlTestRule.setLoadStrategyFactory(loadStrategyFactory);
		abstractNoSqlTestRule
				.setInjectAnnotationProcessor(injectAnnotationProcessor);

		abstractNoSqlTestRule.apply(base, description).evaluate();

		ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
		
		verify(databaseOperation, times(1)).databaseIs(streamCaptor.capture());

		InputStream isContent = streamCaptor.getValue();
		String scriptContent = IOUtils.readFullStream(isContent);
		assertThat(scriptContent, is("Class Annotation"));
		
	}
}

@UsingDataSet(locations = "test2", withSelectiveLocations = { @Selective(identifier = "one", locations = "test3") }, loadStrategy = LoadStrategyEnum.REFRESH)
class MyGlobalAndSelectiveClass {

}

@UsingDataSet(withSelectiveLocations = { @Selective(identifier = "one", locations = "test3") }, loadStrategy = LoadStrategyEnum.REFRESH)
class MySelectiveClass {

}

@UsingDataSet(loadStrategy = LoadStrategyEnum.REFRESH)
class MyTestClass {

}

@UsingDataSet(loadStrategy = LoadStrategyEnum.REFRESH)
class MyUknownClass {

}

@UsingDataSet(locations = "test2", loadStrategy = LoadStrategyEnum.REFRESH)
@ShouldMatchDataSet(location = "test2")
class DefaultClass {

}

@ShouldMatchDataSet(withSelectiveMatcher = { @SelectiveMatcher(identifier = "one", location = "test3") })
class SelectiveDefaultClass {

}

@ShouldMatchDataSet(location = "test2", withSelectiveMatcher = { @SelectiveMatcher(identifier = "one", location = "test3") })
class SelectiveAndLocationClass {

}
