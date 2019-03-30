package com.lordofthejars.nosqlunit.marklogic;

import org.junit.Test;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_APP_PORT;
import static com.lordofthejars.nosqlunit.marklogic.RemoteMarkLogicConfigurationBuilder.remoteMarkLogic;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WhenRemoteMarkLogicConfigurationIsRequired {

    @Test
    public void remote_configuration_redis_should_contain_remote_parameters() {
        MarkLogicConfiguration remoteConfiguration = remoteMarkLogic().host("localhost").build();
        assertThat(remoteConfiguration.getHost(), is("localhost"));
        assertThat(remoteConfiguration.getPort(), is(DEFAULT_APP_PORT));

    }

    @Test(expected = IllegalArgumentException.class)
    public void remote_configuration_redis_should_throw_an_exception_if_no_host_provided() {
        MarkLogicConfiguration remoteConfiguration = remoteMarkLogic().build();
    }
}
