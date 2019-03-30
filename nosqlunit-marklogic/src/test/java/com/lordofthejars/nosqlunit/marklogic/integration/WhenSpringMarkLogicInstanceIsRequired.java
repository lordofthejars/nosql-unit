package com.lordofthejars.nosqlunit.marklogic.integration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration("marklogic-application.xml")
public class WhenSpringMarkLogicInstanceIsRequired extends AbstractMarkLogicSpringTest {

    @Test
    public void connection_manager_should_be_the_one_defined_in_application_context() {
        validateMarkLogicConnection();
    }
}
