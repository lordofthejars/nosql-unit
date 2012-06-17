package com.lordofthejars.nosqlunit.mongodb.integration;

import java.lang.annotation.Annotation;

import com.lordofthejars.nosqlunit.annotation.Selective;

public class SelectiveAnnotationTest implements Annotation, com.lordofthejars.nosqlunit.annotation.Selective {

	private String identifier;
	private String[] locations;
	
	public SelectiveAnnotationTest(String identifier, String[] locations) {
		this.identifier = identifier;
		this.locations = locations;
	}
	
	@Override
	public Class<? extends Annotation> annotationType() {
		return Selective.class;
	}

	@Override
	public String identifier() {
		return identifier;
	}

	@Override
	public String[] locations() {
		return locations;
	}

	
	
}
