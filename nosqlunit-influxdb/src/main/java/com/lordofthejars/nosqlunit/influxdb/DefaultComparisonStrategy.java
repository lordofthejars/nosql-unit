
package com.lordofthejars.nosqlunit.influxdb;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultComparisonStrategy implements InfluxComparisonStrategy {

    @Override
    public boolean compare(final InfluxDbConnectionCallback connection, final InputStream dataset) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

        final Map<String, List<ExpectedPoint>> parsedData = objectMapper.readValue(dataset,
                ExpectedDataSet.TYPE_REFERENCE);
        parsedData.entrySet() //
                .stream() //
                .forEach((final Entry<String, List<ExpectedPoint>> entry) -> {
                    final String measurement = entry.getKey();
                    entry.getValue().stream().forEach(point -> point.setMeasurement(measurement));
                });

        InfluxDbAssertion.strictAssertEquals(new ExpectedDataSet(parsedData), connection.dbClient());

        return true;
    }

    @Override
    public void setIgnoreProperties(final String[] ignoreProperties) {
        // do nothing
    }

}
