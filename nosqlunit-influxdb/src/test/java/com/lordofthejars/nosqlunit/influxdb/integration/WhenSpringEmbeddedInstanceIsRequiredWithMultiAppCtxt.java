
package com.lordofthejars.nosqlunit.influxdb.integration;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextHierarchy({ @ContextConfiguration("embedded-influx-spring-definition.xml"),
    @ContextConfiguration(classes = WhenSpringEmbeddedInstanceIsRequiredWithMultiAppCtxt.MyTestDependency.class) })
public class WhenSpringEmbeddedInstanceIsRequiredWithMultiAppCtxt extends SpringEmbeddedInstanceBase {

    @Autowired
    MyTestDependency myTestDependency;

    @Test
    public void connection_manager_should_be_the_one_defined_in_application_context() {
        validateInfluxConnection();
        Assert.assertNotNull(myTestDependency);
    }

    @Component
    public static class MyTestDependency {
    }

}
