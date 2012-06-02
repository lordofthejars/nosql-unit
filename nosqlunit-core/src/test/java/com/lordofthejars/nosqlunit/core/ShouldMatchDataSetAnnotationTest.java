package com.lordofthejars.nosqlunit.core;

import java.lang.annotation.Annotation;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;

public class ShouldMatchDataSetAnnotationTest implements Annotation, com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet {

	private String[] locations;
	
	
	public ShouldMatchDataSetAnnotationTest(String[] locations) {
		this.locations = locations;
	}
	
	@Override
	public Class<? extends Annotation> annotationType() {
		return ShouldMatchDataSet.class;
	}

	@Override
	public String[] values() {
		return locations;
	}

}
