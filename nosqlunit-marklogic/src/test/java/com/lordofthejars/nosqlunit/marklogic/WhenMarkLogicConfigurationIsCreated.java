package com.lordofthejars.nosqlunit.marklogic;

import org.junit.Test;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfigurationBuilder.marklogic;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WhenMarkLogicConfigurationIsCreated {

    @Test
    public void managed_parameter_values_should_contain_default_values() {
        MarkLogicConfiguration managedConfiguration = marklogic().database("test").build();
        assertThat(managedConfiguration.getHost(), is("localhost"));
        assertThat(managedConfiguration.getPort(), is(8001));
        assertThat(managedConfiguration.getDatabase(), is("test"));
        assertThat(managedConfiguration.getUsername(), is("admin"));
        assertThat(managedConfiguration.getPassword(), is("admin"));
    }
}
