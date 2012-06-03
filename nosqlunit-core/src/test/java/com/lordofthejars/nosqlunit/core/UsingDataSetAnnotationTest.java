package com.lordofthejars.nosqlunit.core;

import java.lang.annotation.Annotation;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;

public class UsingDataSetAnnotationTest implements Annotation, com.lordofthejars.nosqlunit.annotation.UsingDataSet {

	private String[] locations;
	private LoadStrategyEnum loadStrategyEnum;
	
	public UsingDataSetAnnotationTest(LoadStrategyEnum loadStrategyEnum) {
		this.loadStrategyEnum = loadStrategyEnum;
	}
	
	public UsingDataSetAnnotationTest(String[] locations, LoadStrategyEnum loadStrategyEnum) {
		this.loadStrategyEnum = loadStrategyEnum;
		this.locations = locations;
	}
	
	@Override
	public Class<? extends Annotation> annotationType() {
		return UsingDataSet.class;
	}

	@Override
	public String[] locations() {
		return locations;
	}

	@Override
	public LoadStrategyEnum loadStrategy() {
		return loadStrategyEnum;
	}

}
