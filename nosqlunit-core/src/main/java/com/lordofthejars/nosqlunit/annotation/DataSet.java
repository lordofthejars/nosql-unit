package com.lordofthejars.nosqlunit.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface DataSet {

	String[] locations() default {};
	LoadStrategyEnum loadStrategy() default LoadStrategyEnum.CLEAN_INSERT;
	
}
