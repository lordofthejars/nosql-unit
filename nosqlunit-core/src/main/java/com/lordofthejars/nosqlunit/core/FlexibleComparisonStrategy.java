package com.lordofthejars.nosqlunit.core;

/**
 * @author <a mailto="victor.hernandezbermejo@gmail.com">Víctor Hernández</a>
 */
public interface FlexibleComparisonStrategy {

    void setIgnorePropertyValues(String... propertiesToIgnore);
}
