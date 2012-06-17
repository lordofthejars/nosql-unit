package com.lordofthejars.nosqlunit.core;

import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.inject.Inject;

import org.junit.Test;

public class WhenFieldIsInjected {

	@Test
	public void object_should_be_injected_as_field_instance() {
		
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor();
		TestWithFieldInjection testInstance = new TestWithFieldInjection();

		injectAnnotationProcessor.processInjectAnnotation(TestWithFieldInjection.class, testInstance, "Hello");
		
		assertThat(testInstance.getMyInjection(), is("Hello"));
		
	}
	
	@Test
	public void not_instanciable_objects_should_not_be_injected() {
		InjectAnnotationProcessor injectAnnotationProcessor = new InjectAnnotationProcessor();
		TestWithFieldInjection testInstance = new TestWithFieldInjection();

		injectAnnotationProcessor.processInjectAnnotation(TestWithFieldInjection.class, testInstance, new Integer(0));
		
		assertThat(testInstance.getMyInjection(), nullValue());
	}
	
}

class TestWithFieldInjection {
	
	@Inject
	private String myInjection;
	
	public String getMyInjection() {
		return myInjection;
	}
	
}