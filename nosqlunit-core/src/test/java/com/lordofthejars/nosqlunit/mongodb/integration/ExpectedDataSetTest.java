package com.lordofthejars.nosqlunit.mongodb.integration;

import java.lang.annotation.Annotation;

import com.lordofthejars.nosqlunit.annotation.ExpectedDataSet;

public class ExpectedDataSetTest implements Annotation, com.lordofthejars.nosqlunit.annotation.ExpectedDataSet {

	private String[] locations;
	
	
	public ExpectedDataSetTest(String[] locations) {
		this.locations = locations;
	}
	
	@Override
	public Class<? extends Annotation> annotationType() {
		return ExpectedDataSet.class;
	}

	@Override
	public String[] values() {
		return locations;
	}

}
