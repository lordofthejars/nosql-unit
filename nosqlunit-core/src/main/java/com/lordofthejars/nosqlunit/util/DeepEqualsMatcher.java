package com.lordofthejars.nosqlunit.util;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class DeepEqualsMatcher extends TypeSafeMatcher<Object> {

	private Object object;

	private DeepEqualsMatcher(Object object) {
		this.object = object;
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("not deep equals");
	}

	@Override
	protected boolean matchesSafely(Object object) {
		return DeepEquals.deepEquals(this.object, object);
	}

	@Factory
	public static <T> Matcher<Object> deepEquals(Object object) {
		return new DeepEqualsMatcher(object);
	}

}
