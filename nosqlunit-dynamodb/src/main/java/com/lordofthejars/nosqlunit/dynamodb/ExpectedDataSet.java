
package com.lordofthejars.nosqlunit.dynamodb;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.core.type.TypeReference;

public class ExpectedDataSet {

    public static final TypeReference<Map<String, List<Map<String, AttributeValue>>>> TYPE_REFERENCE = new TypeReference<Map<String, List<Map<String, AttributeValue>>>>() {
    };

    private final Map<String, List<Map<String, AttributeValue>>> dataset;

    public ExpectedDataSet(Map<String, List<Map<String, AttributeValue>>> dataset) {
        this.dataset = dataset;
    }

    public Map<String, List<Map<String, AttributeValue>>> getDataset() {
        return dataset;
    }

    public Set<String> getTables() {
        return dataset.keySet();
    }

    public List<Map<String, AttributeValue>> getDataFor(String tableName) {
        return dataset.get(tableName);
    }

}
