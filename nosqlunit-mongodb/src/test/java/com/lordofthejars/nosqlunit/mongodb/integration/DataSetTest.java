package com.lordofthejars.nosqlunit.mongodb.integration;

import java.lang.annotation.Annotation;

import com.lordofthejars.nosqlunit.annotation.DataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;

public class DataSetTest implements Annotation, com.lordofthejars.nosqlunit.annotation.DataSet {

	private String[] locations;
	private LoadStrategyEnum loadStrategyEnum;
	
	public DataSetTest(String[] locations, LoadStrategyEnum loadStrategyEnum) {
		this.loadStrategyEnum = loadStrategyEnum;
		this.locations = locations;
	}
	
	@Override
	public Class<? extends Annotation> annotationType() {
		return DataSet.class;
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
