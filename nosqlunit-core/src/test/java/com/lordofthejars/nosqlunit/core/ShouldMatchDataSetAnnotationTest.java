package com.lordofthejars.nosqlunit.core;

import java.lang.annotation.Annotation;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;

public class ShouldMatchDataSetAnnotationTest implements Annotation, com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet {

	private String location;
	
	public ShouldMatchDataSetAnnotationTest() {
		
	}
	
	public ShouldMatchDataSetAnnotationTest(String location) {
		this.location = location;
	}
	
	@Override
	public Class<? extends Annotation> annotationType() {
		return ShouldMatchDataSet.class;
	}

	@Override
	public String location() {
		return location;
	}

}
