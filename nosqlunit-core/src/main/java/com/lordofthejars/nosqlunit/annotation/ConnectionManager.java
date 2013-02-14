package com.lordofthejars.nosqlunit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(value={ElementType.METHOD,ElementType.CONSTRUCTOR,ElementType.FIELD})
@Retention(value=RetentionPolicy.RUNTIME)
@Documented
public @interface ConnectionManager {

}
