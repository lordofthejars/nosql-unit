package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.ComparisonStrategy;

public interface MarkLogicComparisonStrategy extends ComparisonStrategy<MarkLogicConnectionCallback> {

    /**
     * Since MarkLogic supports different formats it' up to concrete strategy to implement this interface.
     * @param ignoreProperties properties to be ignored
     */
    @Override
    default void setIgnoreProperties(String[] ignoreProperties) {
        //no properties, so nothing to ignore
    }
}
