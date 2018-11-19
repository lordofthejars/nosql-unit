
package com.lordofthejars.nosqlunit.dynamodb;

import static com.lordofthejars.nosqlunit.dynamodb.DynamoDbConfigurationBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class WhenDynamoDbConfigurationIsCreated {

    @Test
    public void managed_parameter_values_should_contain_default_values() {
        DynamoDbConfiguration managedConfiguration = dynamoDb().build();

        assertThat(managedConfiguration.getEndpoint(), is("http://localhost:8000"));
    }

}
