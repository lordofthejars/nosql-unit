package com.lordofthejars.nosqlunit.annotation;

public @interface SelectiveMatcher {

	String identifier() default "";
	String location() default "";
	
}
