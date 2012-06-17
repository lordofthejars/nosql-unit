package com.lordofthejars.nosqlunit.annotation;

public @interface SelectiveMatcher {

	String identifier() default "";
	String locations() default "";
	
}
