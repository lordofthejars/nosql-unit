package com.lordofthejars.nosqlunit.elasticsearch2;

import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.lordofthejars.nosqlunit.elasticsearch2.ManagedElasticsearch.ManagedElasticsearchRuleBuilder.newManagedElasticsearchRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class WhenManagedElasticsearchLifecycleIsManaged {
	private static final String DEFAULT_ELASTICSEARCH_HOME = "/opt/elasticsearch-2.0.0-beta1/";
	private static final String ES_HOME = System.getProperty("ES_HOME", DEFAULT_ELASTICSEARCH_HOME);
	private static final Path ES_HOME_PATH = Paths.get(ES_HOME);

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
		final String oldElasticsearchHome = System.getProperty("ES_HOME");
		if (oldElasticsearchHome == null) {
			System.setProperty("ES_HOME", ES_HOME);
		}

		when(lowLevelElasticSearchOperations.assertThatConnectionToElasticsearchIsPossible(anyString(), anyInt())).thenReturn(true);
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyListOf(String.class))).thenReturn(mockProcess);

		try {
			ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().build();
			managedElasticsearch.managedElasticsearchLifecycleManager.setCommandLineExecutor(commandLineExecutor);
			managedElasticsearch.managedElasticsearchLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
			managedElasticsearch.managedElasticsearchLifecycleManager.setLowLevelElasticSearchOperations(lowLevelElasticSearchOperations);

			managedElasticsearch.before();
			managedElasticsearch.after();

			verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedElasticsearchLifecycleManager.DEFAULT_ELASTICSEARCH_TARGET_PATH,
					getExpectedXCommand());
		} finally {
			if (oldElasticsearchHome == null) {
				System.clearProperty("ES_HOME");
			}
		}
	}

	@Test
	public void managed_elasticsearch_should_be_started_from_elasticsearch_custom_location() throws Throwable {
		when(lowLevelElasticSearchOperations.assertThatConnectionToElasticsearchIsPossible(anyString(), anyInt())).thenReturn(true);
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyListOf(String.class))).thenReturn(mockProcess);

		ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().elasticsearchPath(ES_HOME).build();
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

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyListOf(String.class))).thenReturn(mockProcess);

		ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().elasticsearchPath(ES_HOME).build();
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

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyListOf(String.class))).thenReturn(mockProcess);

		ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().elasticsearchPath(ES_HOME).build();
		managedElasticsearch.managedElasticsearchLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedElasticsearch.managedElasticsearchLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		managedElasticsearch.managedElasticsearchLifecycleManager.setLowLevelElasticSearchOperations(lowLevelElasticSearchOperations);

		managedElasticsearch.before();
		managedElasticsearch.after();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", ManagedElasticsearchLifecycleManager.DEFAULT_PORT), is(false));
	}

	private List<String> getExpectedWindowsCommand() {
		final List<String> expectedCommand = new ArrayList<>();
		expectedCommand.add(ES_HOME_PATH.resolve(Paths.get("bin", "elasticsearch.bat")).toString());

		return expectedCommand;
	}

	private List<String> getExpectedXCommand() {
		final List<String> expectedCommand = new ArrayList<>();
		expectedCommand.add(ES_HOME_PATH.resolve(Paths.get("bin", "elasticsearch")).toString());
		expectedCommand.add(ManagedElasticsearchLifecycleManager.FOREGROUND_OPTION);

		return expectedCommand;
	}
}
