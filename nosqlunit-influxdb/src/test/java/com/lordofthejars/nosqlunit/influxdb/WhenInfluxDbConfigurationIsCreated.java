
package com.lordofthejars.nosqlunit.influxdb;

import static com.lordofthejars.nosqlunit.influxdb.InfluxDbConfigurationBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class WhenInfluxDbConfigurationIsCreated {

    @Test
    public void managed_parameter_values_should_contain_default_values() {
        InfluxDbConfiguration config = influxDb().build();

        assertThat(config.getUrl(), is("http://localhost:8086"));
    }

}
