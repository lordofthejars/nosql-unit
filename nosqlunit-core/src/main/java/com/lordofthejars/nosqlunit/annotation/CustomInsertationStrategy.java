package com.lordofthejars.nosqlunit.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CustomInsertationStrategy {

	Class<? extends com.lordofthejars.nosqlunit.core.InsertationStrategy<?>> insertationStrategy();
	
}
