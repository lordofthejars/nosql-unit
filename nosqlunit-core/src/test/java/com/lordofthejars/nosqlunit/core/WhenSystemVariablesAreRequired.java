package com.lordofthejars.nosqlunit.core;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Test;

import com.lordofthejars.nosqlunit.env.SystemEnvironmentVariables;

public class WhenSystemVariablesAreRequired {

	@Test
	public void property_variable_should_be_returned() {
	
		System.setProperty("MY_VAR", "alex");
		
		String propertyVariable = SystemEnvironmentVariables.getPropertyVariable("MY_VAR");
		assertThat(propertyVariable, is("alex"));
		
		System.clearProperty("MY_VAR");
		
	}

	@Test
	public void property_variable_should_be_returned_if_no_environment_variable() {
		
		System.setProperty("MY_VAR", "alex");
		
		String propertyVariable = SystemEnvironmentVariables.getEnvironmentOrPropertyVariable("MY_VAR");
		assertThat(propertyVariable, is("alex"));
		
		System.clearProperty("MY_VAR");
		
	}
	
}
