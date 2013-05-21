package com.lordofthejars.nosqlunit.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

public class PropertyGetterTest {

	private static class TestClass {
		List list = Arrays.asList(1, 2, 3);
	}
	
	private final TestClass testInstance = new TestClass();
	
	@Test
	public void propertyByType_sameTypes() {
		PropertyGetter<List> propertyGetter = new PropertyGetter<List>();
		assertThat(propertyGetter.propertyByType(testInstance, List.class), sameInstance(testInstance.list));
	}
	
	@Test
	public void propertyByType_findSuperType() {
		PropertyGetter<Collection> propertyGetter = new PropertyGetter<Collection>();
		assertThat(propertyGetter.propertyByType(testInstance, Collection.class), sameInstance((Collection) testInstance.list));
	}
	
	@Test
	public void propertyByType_findSubType() {
		PropertyGetter<ArrayList> propertyGetter = new PropertyGetter<ArrayList>();
		assertThat(propertyGetter.propertyByType(testInstance, ArrayList.class), nullValue());
	}
}
