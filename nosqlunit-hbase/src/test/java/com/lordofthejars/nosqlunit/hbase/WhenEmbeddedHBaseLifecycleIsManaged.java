package com.lordofthejars.nosqlunit.hbase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static com.lordofthejars.nosqlunit.hbase.EmbeddedHBase.EmbeddedHBaseRuleBuilder.newEmbeddedHBaseRule;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.core.ConnectionManagement;

public class WhenEmbeddedHBaseLifecycleIsManaged {

	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = HConstants.DEFAULT_MASTER_PORT;
	protected static final String TARGET_DIRECTORY = HBaseTestingUtility.DEFAULT_BASE_TEST_DIRECTORY;
	
	@Test
	public void hbase_should_delete_target_directory() throws Throwable {
		
		EmbeddedHBaseStarter embeddedHBaseStarter = mock(EmbeddedHBaseStarter.class);
		HBaseTestingUtility hBaseTestingUtility = mock(HBaseTestingUtility.class);
		when(hBaseTestingUtility.getConfiguration()).thenReturn(HBaseConfiguration.create());
		when(embeddedHBaseStarter.startSingleMiniCluster(any(Configuration.class))).thenReturn(hBaseTestingUtility);

		EmbeddedHBase embeddedHBase = newEmbeddedHBaseRule().build();
		embeddedHBase.embeddedHBaseLifecycleManager.setEmbeddedHBaseStarter(embeddedHBaseStarter);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, PORT), is(true));
				assertThat(EmbeddedHBaseInstances.getInstance().getDefaultConfiguration(), notNullValue());
			}
		};
		
		
		Statement decotedStatement = embeddedHBase.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		File targetDirectory = new File(TARGET_DIRECTORY);
		
		assertThat(targetDirectory.exists(), is(false));
		
		
	}
	
	@Test
	public void hbase_should_start_embedded_and_working() throws Throwable {

		EmbeddedHBaseStarter embeddedHBaseStarter = mock(EmbeddedHBaseStarter.class);
		HBaseTestingUtility hBaseTestingUtility = mock(HBaseTestingUtility.class);
		when(hBaseTestingUtility.getConfiguration()).thenReturn(HBaseConfiguration.create());
		when(embeddedHBaseStarter.startSingleMiniCluster(any(Configuration.class))).thenReturn(hBaseTestingUtility);
		
		EmbeddedHBase embeddedHBase = newEmbeddedHBaseRule().build();
		embeddedHBase.embeddedHBaseLifecycleManager.setEmbeddedHBaseStarter(embeddedHBaseStarter);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, PORT), is(true));
				assertThat(EmbeddedHBaseInstances.getInstance().getDefaultConfiguration(), notNullValue());
			}
		};
		
		
		Statement decotedStatement = embeddedHBase.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, PORT), is(false));
		assertThat(EmbeddedHBaseInstances.getInstance().getDefaultConfiguration(), nullValue());
		
	}
	
	@Test
	public void simulataneous_hbase_should_start_only_one_instance_for_location() throws Throwable {

		final EmbeddedHBaseStarter embeddedHBaseStarter = mock(EmbeddedHBaseStarter.class);
		HBaseTestingUtility hBaseTestingUtility = mock(HBaseTestingUtility.class);
		when(hBaseTestingUtility.getConfiguration()).thenReturn(HBaseConfiguration.create());
		when(embeddedHBaseStarter.startSingleMiniCluster(any(Configuration.class))).thenReturn(hBaseTestingUtility);
		
		EmbeddedHBase embeddedHBase = newEmbeddedHBaseRule().build();
		embeddedHBase.embeddedHBaseLifecycleManager.setEmbeddedHBaseStarter(embeddedHBaseStarter);
		
		Statement noStatement = new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
			
				EmbeddedHBase defaultEmbeddedHBase = newEmbeddedHBaseRule().build();
				defaultEmbeddedHBase.embeddedHBaseLifecycleManager.setEmbeddedHBaseStarter(embeddedHBaseStarter);
				
				Statement defaultNoStatement = new Statement() {
					
					@Override
					public void evaluate() throws Throwable {
						assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, PORT), is(true));
						assertThat(EmbeddedHBaseInstances.getInstance().getDefaultConfiguration(), notNullValue());
					}
				};
				
				Statement defaultStatement = defaultEmbeddedHBase.apply(defaultNoStatement, Description.EMPTY);
				defaultStatement.evaluate();
				
				assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, PORT), is(true));
				assertThat(EmbeddedHBaseInstances.getInstance().getDefaultConfiguration(), notNullValue());
				
			}
		};
		
		Statement decotedStatement = embeddedHBase.apply(noStatement, Description.EMPTY);
		decotedStatement.evaluate();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(LOCALHOST, PORT), is(false));
		assertThat(EmbeddedHBaseInstances.getInstance().getDefaultConfiguration(), nullValue());
		
	}
	
}
