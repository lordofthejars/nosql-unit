
package com.lordofthejars.nosqlunit.influxdb;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;

public class ExpectedDataSet {

    public static final TypeReference<Map<String, List<ExpectedPoint>>> TYPE_REFERENCE = new TypeReference<Map<String, List<ExpectedPoint>>>() {
    };

    private final Map<String, List<ExpectedPoint>> dataset;

    public ExpectedDataSet(final Map<String, List<ExpectedPoint>> dataset) {
        this.dataset = dataset;
    }

    public Map<String, List<ExpectedPoint>> getDataset() {
        return dataset;
    }

    public Set<String> getTables() {
        return dataset.keySet();
    }

    public List<ExpectedPoint> getDataFor(final String tableName) {
        return dataset.get(tableName);
    }

}
