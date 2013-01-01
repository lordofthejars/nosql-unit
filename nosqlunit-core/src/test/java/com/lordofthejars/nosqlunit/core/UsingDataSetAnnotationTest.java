package com.lordofthejars.nosqlunit.core;

import java.lang.annotation.Annotation;

import com.lordofthejars.nosqlunit.annotation.Selective;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;

public class UsingDataSetAnnotationTest implements Annotation, com.lordofthejars.nosqlunit.annotation.UsingDataSet {

	private String[] locations;
	private LoadStrategyEnum loadStrategyEnum;
	private Selective[] selectiveLocations;
	
	public UsingDataSetAnnotationTest(LoadStrategyEnum loadStrategyEnum) {
		this.loadStrategyEnum = loadStrategyEnum;
	}
	
	public UsingDataSetAnnotationTest(LoadStrategyEnum loadStrategyEnum, Selective[] selective) {
		this.loadStrategyEnum = loadStrategyEnum;
		this.selectiveLocations = selective;
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

	@Override
	public Selective[] withSelectiveLocations() {
		return this.selectiveLocations;
	}

}
