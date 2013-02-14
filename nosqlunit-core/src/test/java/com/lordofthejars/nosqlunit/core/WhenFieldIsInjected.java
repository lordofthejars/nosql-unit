package com.lordofthejars.nosqlunit.core;

import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.inject.Inject;
import javax.inject.Named;

import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.ByContainer;
import com.lordofthejars.nosqlunit.annotation.ConnectionManager;

public class WhenFieldIsInjected {

	@Test
	public void named_object_should_Not_be_injected_if_is_named_with_not_current_identifier() {
		
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor("two");
		TestWithNamedFieldInjection testInstance = new TestWithNamedFieldInjection();

		injectAnnotationProcessor.processInjectAnnotation(TestWithNamedFieldInjection.class, testInstance, "Hello");
		
		assertThat(testInstance.getMyInjection(), is("Hello"));
		assertThat(testInstance.getMySecondInjection(), nullValue());
		
	}
	
	@Test
	public void named_object_should_be_injected_if_is_named_without_identifier() {
		
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor("one");
		TestWithNamedFieldInjection testInstance = new TestWithNamedFieldInjection();

		injectAnnotationProcessor.processInjectAnnotation(TestWithNamedFieldInjection.class, testInstance, "Hello");
		
		assertThat(testInstance.getMyInjection(), is("Hello"));
		
	}
	
	@Test
	public void named_object_should_be_injected_if_is_named_with_current_identifier() {
		
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor("one");
		TestWithNamedFieldInjection testInstance = new TestWithNamedFieldInjection();

		injectAnnotationProcessor.processInjectAnnotation(TestWithNamedFieldInjection.class, testInstance, "Hello");
		
		assertThat(testInstance.getMySecondInjection(), is("Hello"));
		
	}
	
	@Test
	public void object_should_be_injected_as_field_instance() {
		
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor("1");
		TestWithFieldInjection testInstance = new TestWithFieldInjection();

		injectAnnotationProcessor.processInjectAnnotation(TestWithFieldInjection.class, testInstance, "Hello");
		
		assertThat(testInstance.getMyInjection(), is("Hello"));
		
	}
	
	@Test
	public void not_instanciable_objects_should_not_be_injected() {
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor("1");
		TestWithFieldInjection testInstance = new TestWithFieldInjection();

		injectAnnotationProcessor.processInjectAnnotation(TestWithFieldInjection.class, testInstance, new Integer(0));
		
		assertThat(testInstance.getMyInjection(), nullValue());
	}
	
	@Test
	public void by_container_object_should_Not_be_injected() {
		
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor("1");
		TestWithInjectionByContainer testInstance = new TestWithInjectionByContainer();
		
		injectAnnotationProcessor.processInjectAnnotation(TestWithInjectionByContainer.class, testInstance, "Hello");
		assertThat(testInstance.getMyInjection(), is(nullValue()));
		
	}
	
	@Test
	public void object_should_be_injected_as_field_instance_with_connection_manager() {
		
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor("1");
		TestWithFieldConnectionManager testInstance = new TestWithFieldConnectionManager();

		injectAnnotationProcessor.processInjectAnnotation(TestWithFieldConnectionManager.class, testInstance, "Hello");
		
		assertThat(testInstance.getMyInjection(), is("Hello"));
		
	}
	
}

class TestWithInjectionByContainer {
	
	@Inject
	@ByContainer
	private String myInjection;
	
	public String getMyInjection() {
		return myInjection;
	}
	
}

class TestWithNamedFieldInjection {
	
	@Named
	@Inject
	private String myInjection;
	
	@Named("one")
	@Inject
	private String mySecondInjection;
	
	public String getMySecondInjection() {
		return mySecondInjection;
	}
	
	public String getMyInjection() {
		return myInjection;
	}
	
}

class TestWithFieldConnectionManager {
	
	@ConnectionManager
	private String myInjection;
	
	public String getMyInjection() {
		return myInjection;
	}
	
}

class TestWithFieldInjection {
	
	@Inject
	private String myInjection;
	
	public String getMyInjection() {
		return myInjection;
	}
	
}