package com.lordofthejars.nosqlunit.marklogic;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The 'flexible' strategy is implemented for JSON and XML format only since these are the only
 * structured formats supported.
 */
public class MarkLogicFlexibleComparisonStrategy extends DefaultComparisonStrategy {

    private String[] ignorePropertyValues = new String[0];

    @Override
    public void setIgnoreProperties(String[] ignorePropertyValues) {
        this.ignorePropertyValues = ignorePropertyValues;
    }

    @Override
    protected XmlComparisonStrategy xmlComparisonStrategy() {
        XmlComparisonStrategy result = new XmlComparisonStrategy();
        result.setIgnoreProperties(ignorePropertyValues);
        return result;
    }

    @Override
    protected JsonComparisonStrategy jsonComparisonStrategy(ObjectMapper mapper) {
        JsonComparisonStrategy result = new JsonComparisonStrategy(mapper);
        result.setIgnoreProperties(ignorePropertyValues);
        return result;
    }
}
