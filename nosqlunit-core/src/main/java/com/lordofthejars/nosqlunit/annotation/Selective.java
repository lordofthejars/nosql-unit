package com.lordofthejars.nosqlunit.annotation;

public @interface Selective {

	String identifier() default "";
	String[] locations() default {};
	
}
