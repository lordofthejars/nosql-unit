package com.lordofthejars.nosqlunit.hbase;

import static com.lordofthejars.nosqlunit.hbase.EmbeddedHBaseConfigurationBuilder.newEmbeddedHBaseConfiguration;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


public class WhenEmbeddedConfigurationIsRequired {

	@Test
	public void in_memory_configuration_should_use_default_embedded_instance() {
		
		Configuration configuration = mock(Configuration.class);
		EmbeddedHBaseInstances.getInstance().addHBaseConfiguration(configuration, "a");
		
		EmbeddedHBaseConfigurationBuilder embeddedHBaseConfiguration = newEmbeddedHBaseConfiguration();
		HBaseConfiguration hBaseConfiguration = embeddedHBaseConfiguration.build();
		EmbeddedHBaseInstances.getInstance().removeHBaseConfiguration("a");
		
		assertThat(hBaseConfiguration.getConfiguration(), is(configuration));
		
	}
	
	@Test
	public void in_memory_configuration_should_use_targeted_instance() {
		
		Configuration configuration1 = mock(Configuration.class);
		EmbeddedHBaseInstances.getInstance().addHBaseConfiguration(configuration1, "a");
		
		Configuration configuration2 = mock(Configuration.class);
		EmbeddedHBaseInstances.getInstance().addHBaseConfiguration(configuration2, "b");
		
		EmbeddedHBaseConfigurationBuilder embeddedHBaseConfiguration = newEmbeddedHBaseConfiguration();
		HBaseConfiguration hBaseConfiguration = embeddedHBaseConfiguration.buildFromTargetPath("a");
		EmbeddedHBaseInstances.getInstance().removeHBaseConfiguration("a");
		EmbeddedHBaseInstances.getInstance().removeHBaseConfiguration("b");
		
		assertThat(hBaseConfiguration.getConfiguration(), is(configuration1));
		
	}
	
	@Test(expected=IllegalStateException.class)
	public void in_memory_configuration_should_throw_an_exception_if_no_default_embedded() {
		
		EmbeddedHBaseConfigurationBuilder embeddedHBaseConfiguration = newEmbeddedHBaseConfiguration();
		embeddedHBaseConfiguration.build();
		
	}
	
	@Test(expected=IllegalStateException.class)
	public void in_memory_configuration_should_throw_an_exception_if_no_targeted_instance() {
		
		EmbeddedHBaseConfigurationBuilder embeddedHBaseConfiguration = newEmbeddedHBaseConfiguration();
		embeddedHBaseConfiguration.buildFromTargetPath("a");
		
	}
	
}
