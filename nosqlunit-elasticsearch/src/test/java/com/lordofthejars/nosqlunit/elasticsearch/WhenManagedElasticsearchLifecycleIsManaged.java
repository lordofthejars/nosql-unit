package com.lordofthejars.nosqlunit.elasticsearch;

import static com.lordofthejars.nosqlunit.elasticsearch.ManagedElasticsearch.ManagedElasticsearchRuleBuilder.newManagedElasticsearchRule;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;


public class WhenManagedElasticsearchLifecycleIsManaged {

	@Mock
	private OperatingSystemResolver operatingSystemResolver;

	@Mock
	private LowLevelElasticSearchOperations lowLevelElasticSearchOperations;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void managed_elasticsearch_should_be_started_from_elasticsearch_home() throws Throwable {
		
		System.setProperty("ES_HOME", "/opt/elasticsearch-0.20.5");

		when(lowLevelElasticSearchOperations.assertThatConnectionToElasticsearchIsPossible(anyString(), anyInt())).thenReturn(true);
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().build();
		managedElasticsearch.managedElasticsearchLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedElasticsearch.managedElasticsearchLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		managedElasticsearch.managedElasticsearchLifecycleManager.setLowLevelElasticSearchOperations(lowLevelElasticSearchOperations);

		managedElasticsearch.before();
		managedElasticsearch.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedElasticsearchLifecycleManager.DEFAULT_ELASTICSEARCH_TARGET_PATH,
				getExpectedXCommand());
		
		System.clearProperty("ES_HOME");
		
	}
	
	@Test
	public void managed_elasticsearch_should_be_started_from_elasticsearch_custom_location() throws Throwable {
		
		when(lowLevelElasticSearchOperations.assertThatConnectionToElasticsearchIsPossible(anyString(), anyInt())).thenReturn(true);
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().elasticsearchPath("/opt/elasticsearch-0.20.5").build();
		managedElasticsearch.managedElasticsearchLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedElasticsearch.managedElasticsearchLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		managedElasticsearch.managedElasticsearchLifecycleManager.setLowLevelElasticSearchOperations(lowLevelElasticSearchOperations);

		managedElasticsearch.before();
		managedElasticsearch.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedElasticsearchLifecycleManager.DEFAULT_ELASTICSEARCH_TARGET_PATH,
				getExpectedXCommand());
		
	}
	
	@Test
	public void managed_elasticsearch_should_be_started_from_windows_custom_location() throws Throwable {
		
		when(lowLevelElasticSearchOperations.assertThatConnectionToElasticsearchIsPossible(anyString(), anyInt())).thenReturn(true);
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.WINDOWS_7);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().elasticsearchPath("/opt/elasticsearch-0.20.5").build();
		managedElasticsearch.managedElasticsearchLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedElasticsearch.managedElasticsearchLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		managedElasticsearch.managedElasticsearchLifecycleManager.setLowLevelElasticSearchOperations(lowLevelElasticSearchOperations);

		managedElasticsearch.before();
		managedElasticsearch.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedElasticsearchLifecycleManager.DEFAULT_ELASTICSEARCH_TARGET_PATH,
				getExpectedWindowsCommand());
		
	}
	
	@Test
	public void managed_elasticsearch_should_be_stopped() throws Throwable {
		
		when(lowLevelElasticSearchOperations.assertThatConnectionToElasticsearchIsPossible(anyString(), anyInt())).thenReturn(true);
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().elasticsearchPath("/opt/elasticsearch-0.20.5").build();
		managedElasticsearch.managedElasticsearchLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedElasticsearch.managedElasticsearchLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		managedElasticsearch.managedElasticsearchLifecycleManager.setLowLevelElasticSearchOperations(lowLevelElasticSearchOperations);

		managedElasticsearch.before();
		managedElasticsearch.after();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", ManagedElasticsearchLifecycleManager.DEFAULT_PORT), is(false));
		
		
	}
	
	private List<String> getExpectedWindowsCommand() {
		
		List<String> expectedCommand = new ArrayList<String>();
		
		expectedCommand.add("/opt/elasticsearch-0.20.5/bin/elasticsearch.bat");
		
		return expectedCommand;
		
	}
	
	private List<String> getExpectedXCommand() {
		
		List<String> expectedCommand = new ArrayList<String>();
		
		expectedCommand.add("/opt/elasticsearch-0.20.5/bin/elasticsearch");
		expectedCommand.add(ManagedElasticsearchLifecycleManager.FOREGROUND_OPTION);
		
		return expectedCommand;
		
	}
	
}
