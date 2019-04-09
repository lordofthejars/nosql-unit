package com.lordofthejars.nosqlunit.marklogic.integration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@ContextHierarchy({
        @ContextConfiguration("marklogic-application.xml"),
        @ContextConfiguration(classes = WhenSpringMarkLogicIsRequiredWithMultiAppContexts.MarkLogicConfig.class)
})
@TestPropertySource(locations = "secondary-marklogic.properties")
public class WhenSpringMarkLogicIsRequiredWithMultiAppContexts extends AbstractMarkLogicSpringTest {

    @Autowired
    private MarkLogicConfig childConfig;

    @Test
    public void connection_manager_should_be_the_one_defined_in_application_context() {
        validateMarkLogicConnection();
        assertNotNull(childConfig);
        assertEquals("localhost", childConfig.host);
        assertEquals(9000, childConfig.port);
        assertEquals("some-other", childConfig.database);
    }

    @Configuration
    public static class MarkLogicConfig {

        @Value("${marklogic.host}")
        private String host;

        @Value("${marklogic.port}")
        private int port;

        @Value("${marklogic.database:#{null}}")
        private String database;

        @Value("${marklogic.user:admin}")
        private String user;

        @Value("${marklogic.password:admin}")
        private String password;
    }
}
