package com.lordofthejars.nosqlunit.hbase;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static com.lordofthejars.nosqlunit.hbase.EmbeddedHBaseConfigurationBuilder.newEmbeddedHBaseConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class WhenEmbeddedHBaseRuleConfigurationIsCreated {

	@Test
	public void embedded_configuration_should_set_default_embedded_instance_into_configuration_object() {
		
		Configuration configuration = mock(Configuration.class);
		
		EmbeddedHBaseInstances.getInstance().addHBaseConfiguration(configuration, "a");
		
		HBaseConfiguration hBaseConfiguration = newEmbeddedHBaseConfiguration().build();
		assertThat(hBaseConfiguration.getConfiguration(), is(configuration));
		
		EmbeddedHBaseInstances.getInstance().removeHBaseConfiguration("a");
		
	}
	
	@Test
	public void embedded_configuration_should_set_targeted_embedded_instance_into_configuration_object() {
		
		Configuration configuration = mock(Configuration.class);
		
		EmbeddedHBaseInstances.getInstance().addHBaseConfiguration(configuration, "a");
		
		HBaseConfiguration hBaseConfiguration = newEmbeddedHBaseConfiguration().buildFromTargetPath("a");
		assertThat(hBaseConfiguration.getConfiguration(), is(configuration));
		
		EmbeddedHBaseInstances.getInstance().removeHBaseConfiguration("a");
		
	}
	
	@Test(expected=IllegalStateException.class)
	public void embedded_configuration_should_throw_an_exception_if_no_configuration_object() {
		
		HBaseConfiguration hBaseConfiguration = newEmbeddedHBaseConfiguration().buildFromTargetPath("a");
		
	}
	
}
