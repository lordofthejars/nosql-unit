
package com.lordofthejars.nosqlunit.dynamodb.integration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "embedded-dynamo-spring-definition.xml")
public class WhenSpringEmbeddedInstanceIsRequired extends SpringEmbeddedInstanceBase {

    @Test
    public void connection_manager_should_be_the_one_defined_in_application_context() {
        validateDynamoConnection();
    }

}
