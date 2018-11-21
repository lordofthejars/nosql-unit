
package com.lordofthejars.nosqlunit.dynamodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultComparisonStrategy implements DynamoComparisonStrategy {

    @Override
    public boolean compare(DynamoDbConnectionCallback connection, InputStream dataset) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

        Map<String, List<Map<String, AttributeValue>>> parsedData = objectMapper.readValue(dataset,
                ExpectedDataSet.TYPE_REFERENCE);

        DynamoDbAssertion.strictAssertEquals(new ExpectedDataSet(parsedData), connection.dbClient());

        return true;
    }

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
    }

}
