package com.lordofthejars.nosqlunit.marklogic;

import org.junit.Test;

import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicConfigurationBuilder.marklogic;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WhenManagedMarkLogicConfigurationIsCreated {

    @Test
    public void managed_parameter_values_should_contain_default_values() {
        MarkLogicConfiguration managedConfiguration = marklogic().port(8000).database("Documents").build();
        assertThat(managedConfiguration.getHost(), is("localhost"));
        assertThat(managedConfiguration.getPort(), is(8000));
        assertThat(managedConfiguration.getDatabase(), is("Documents"));
        assertThat(managedConfiguration.getUsername(), is("admin"));
        assertThat(managedConfiguration.getPassword(), is("admin"));
    }
}
