
package com.lordofthejars.nosqlunit.dynamodb;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;

public class WhenDynamoDbOperationsAreRequired {

    private static final String DATA = "" + "{" + "\"table1\": " + "	["
            + "		{\"id\": {\"N\": \"1\"}, \"code\": {\"S\": \"JSON dataset 1\"} }" + "	]," + "\"table2\": "
            + "	[" + "     {\"id\": {\"N\": \"2\"}, \"code\": {\"S\": \"JSON dataset 2\"} }" + "	]" + "}";

    @Mock
    private AmazonDynamoDB dynamo;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(dynamo.putItem(any())).thenReturn(new PutItemResult());
    }

    @Test
    public void insert_opertation_should_add_data_into_tables() throws UnsupportedEncodingException {

        DynamoOperation dynamoOperation = new DynamoOperation(dynamo, new DynamoDbConfiguration("localhost"));
        dynamoOperation.insert(new ByteArrayInputStream(DATA.getBytes("UTF-8")));

        final ArgumentCaptor<PutItemRequest> insertCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamo, times(2)).putItem(insertCaptor.capture());

        List<PutItemRequest> allValues = insertCaptor.getAllValues();
        assertThat(allValues, hasSize(2));

        for (int i = 0; i < allValues.size(); i++) {
            PutItemRequest putItemRequest = allValues.get(i);
            assertThat(putItemRequest, is(notNullValue()));
            assertThat(putItemRequest.getTableName(), is("table" + (i + 1)));
            assertThat(putItemRequest.getItem(), is(notNullValue()));
            assertThat(putItemRequest.getItem(), hasEntry("id", new AttributeValue().withN(String.valueOf(i + 1))));
            assertThat(putItemRequest.getItem(), hasEntry("code", new AttributeValue("JSON dataset " + (i + 1))));
        }
    }

}
