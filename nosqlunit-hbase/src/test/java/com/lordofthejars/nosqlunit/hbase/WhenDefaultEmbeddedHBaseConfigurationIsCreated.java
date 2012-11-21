package com.lordofthejars.nosqlunit.hbase;

import static com.lordofthejars.nosqlunit.hbase.EmbeddedHBase.EmbeddedHBaseRuleBuilder.newEmbeddedHBaseRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class WhenDefaultEmbeddedHBaseConfigurationIsCreated {

	@Test
	public void configuration_object_should_contain_default_values() {
		
		EmbeddedHBase embeddedHBase = newEmbeddedHBaseRule().build();
		
		assertThat(embeddedHBase.embeddedHBaseLifecycleManager.getHost(), is("127.0.0.1"));
		assertThat(embeddedHBase.embeddedHBaseLifecycleManager.getPort(), is(60000));
		assertThat(embeddedHBase.embeddedHBaseLifecycleManager.getFilePermissions(), is("775"));
		
	}
	
	@Test
	public void configuration_object_should_set_file_permissions() {
		
		EmbeddedHBase embeddedHBase = newEmbeddedHBaseRule().dirPermissions("700").build();
		
		assertThat(embeddedHBase.embeddedHBaseLifecycleManager.getHost(), is("127.0.0.1"));
		assertThat(embeddedHBase.embeddedHBaseLifecycleManager.getPort(), is(60000));
		assertThat(embeddedHBase.embeddedHBaseLifecycleManager.getFilePermissions(), is("700"));
		
	}
	
}
