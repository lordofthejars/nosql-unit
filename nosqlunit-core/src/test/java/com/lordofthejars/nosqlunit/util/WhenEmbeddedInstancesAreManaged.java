package com.lordofthejars.nosqlunit.util;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import org.junit.Test;

public class WhenEmbeddedInstancesAreManaged {

	@Test
	public void embedded_instance_should_be_registered() {
		
		EmbeddedInstances<String> embeddedInstances = new EmbeddedInstances<String>();
		embeddedInstances.addEmbeddedInstance("my_embedded", "my_target");
		
		assertThat(embeddedInstances.getEmbeddedByTargetPath("my_target"), is("my_embedded"));
		
	}
	
	@Test
	public void embedded_instance_should_be_unregistered_at_the_end() {
		
		EmbeddedInstances<String> embeddedInstances = new EmbeddedInstances<String>();
		embeddedInstances.addEmbeddedInstance("my_embedded", "my_target");
		embeddedInstances.removeEmbeddedInstance("my_target");
		
		assertThat(embeddedInstances.getEmbeddedByTargetPath("my_target"), is(nullValue()));
	}

	@Test
	public void embedded_instance_should_return_the_first_instance_in_case_of_no_target_path() {
		
		EmbeddedInstances<String> embeddedInstances = new EmbeddedInstances<String>();
		embeddedInstances.addEmbeddedInstance("my_embedded", "my_target");
		
		assertThat(embeddedInstances.getDefaultEmbeddedInstance(),  is("my_embedded"));
		
	}
	
}
