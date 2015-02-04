package com.lordofthejars.nosqlunit.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allow the user define the properties that should be ignored when checking
 * the expected objects.
 *
 * Accepts two formats for property definition:
 * <li>
 *     <ol>
 *         collection.property : When is defined both collection name and property
 *         name, the exclusion only will affect to the indicated collection.
 *         e.g: With @IgnorePropertyValue(properties = {"book.date"}), the property
 *         will be ignored in each object of the other collection. If other objects
 *         have the property 'date' it won't be ignored.
 *     </ol>
 *     <ol>
 *         property : When only is defined the property name, it will be excluded
 *         for all objects in any expected collection.
 *         e.g: With @IgnorePropertyValue(properties = {"date"}), the property 'date'
 *         will be ignored in each object, no matter the collection.
 *     </ol>
 * </li>
 *
 * @author <a mailto="victor.hernandezbermejo@gmail.com">Víctor Hernández</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface IgnorePropertyValue {

    String[] properties() default {};
}
