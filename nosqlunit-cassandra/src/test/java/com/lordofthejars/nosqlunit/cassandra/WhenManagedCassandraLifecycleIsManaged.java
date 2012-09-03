package com.lordofthejars.nosqlunit.cassandra;

import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandra.ManagedCassandraRuleBuilder.newManagedCassandraRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.service.CassandraHost;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;

public class WhenManagedCassandraLifecycleIsManaged {

	@Mock
	private OperatingSystemResolver operatingSystemResolver;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void managed_cassandra_should_be_registered_and_started_with_default_parameters() throws Throwable {
		
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCassandra managedCassandra = newManagedCassandraRule().build();
		
		managedCassandra.setCommandLineExecutor(commandLineExecutor);
		managedCassandra.setOperatingSystemResolver(operatingSystemResolver);
		
		managedCassandra.before();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", CassandraHost.DEFAULT_PORT), is(true));

		managedCassandra.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", CassandraHost.DEFAULT_PORT), is(false));
		
		System.clearProperty("CASSANDRA_HOME");
		
	}
	
	@Test
	public void managed_cassandra_should_be_registered_and_started_with_custom_parameters() throws Throwable {
		
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCassandra managedCassandra = newManagedCassandraRule().port(9191).build();
		
		managedCassandra.setCommandLineExecutor(commandLineExecutor);
		managedCassandra.setOperatingSystemResolver(operatingSystemResolver);
		
		managedCassandra.before();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));

		managedCassandra.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(false));
		
		System.clearProperty("CASSANDRA_HOME");
		
	}
	
	@Test
	public void simulataneous_cassandra_should_start_only_one_instance() throws Throwable {
		
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCassandra managedCassandra = newManagedCassandraRule().port(9191).build();
		
		managedCassandra.setCommandLineExecutor(commandLineExecutor);
		managedCassandra.setOperatingSystemResolver(operatingSystemResolver);
		
		managedCassandra.before();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));

		
		ManagedCassandra managedCassandra2 = newManagedCassandraRule().port(9191).build();
		
		managedCassandra2.setCommandLineExecutor(commandLineExecutor);
		managedCassandra2.setOperatingSystemResolver(operatingSystemResolver);
		
		managedCassandra2.before();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));
		managedCassandra2.after();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));
		
		managedCassandra.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(false));
		
		System.clearProperty("CASSANDRA_HOME");
		
	}
	
	@Test
	public void cassandra_should_be_started_in_Linux() throws Throwable {
		
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCassandra managedCassandra = newManagedCassandraRule().port(9191).build();
		
		managedCassandra.setCommandLineExecutor(commandLineExecutor);
		managedCassandra.setOperatingSystemResolver(operatingSystemResolver);
		
		managedCassandra.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/cassandra"+File.separatorChar+ManagedCassandra.CASSANDRA_BINARY_DIRECTORY+File.separatorChar+ManagedCassandra.CASSANDRA_EXECUTABLE_X);
		expectedCommand.add(ManagedCassandra.FOREGROUND_ARGUMENT_NAME);
		
		managedCassandra.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedCassandra.DEFAULT_CASSANDRA_TARGET_PATH, expectedCommand);
		
		System.clearProperty("CASSANDRA_HOME");
		
	}
	
	@Test
	public void cassandra_should_be_started_in_Linux_from_custom_location() throws Throwable {
		
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCassandra managedCassandra = newManagedCassandraRule().cassandraPath("/opt/cassandra").port(9191).build();
		
		managedCassandra.setCommandLineExecutor(commandLineExecutor);
		managedCassandra.setOperatingSystemResolver(operatingSystemResolver);
		
		managedCassandra.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/cassandra"+File.separatorChar+ManagedCassandra.CASSANDRA_BINARY_DIRECTORY+File.separatorChar+ManagedCassandra.CASSANDRA_EXECUTABLE_X);
		expectedCommand.add(ManagedCassandra.FOREGROUND_ARGUMENT_NAME);
		
		managedCassandra.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedCassandra.DEFAULT_CASSANDRA_TARGET_PATH, expectedCommand);
		
		
	}
	
	@Test
	public void cassandra_should_be_started_in_MacOsX() throws Throwable {
		
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.MAC_OSX);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCassandra managedCassandra = newManagedCassandraRule().port(9191).build();
		
		managedCassandra.setCommandLineExecutor(commandLineExecutor);
		managedCassandra.setOperatingSystemResolver(operatingSystemResolver);
		
		managedCassandra.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/cassandra"+File.separatorChar+ManagedCassandra.CASSANDRA_BINARY_DIRECTORY+File.separatorChar+ManagedCassandra.CASSANDRA_EXECUTABLE_X);
		expectedCommand.add(ManagedCassandra.FOREGROUND_ARGUMENT_NAME);
		
		managedCassandra.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedCassandra.DEFAULT_CASSANDRA_TARGET_PATH, expectedCommand);
		
		System.clearProperty("CASSANDRA_HOME");
		
	}

	@Test
	public void cassandra_should_be_started_in_Windows() throws Throwable {
		
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.WINDOWS_7);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCassandra managedCassandra = newManagedCassandraRule().port(9191).build();
		
		managedCassandra.setCommandLineExecutor(commandLineExecutor);
		managedCassandra.setOperatingSystemResolver(operatingSystemResolver);
		
		managedCassandra.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/cassandra"+File.separatorChar+ManagedCassandra.CASSANDRA_BINARY_DIRECTORY+File.separatorChar+ManagedCassandra.CASSANDRA_EXECUTABLE_W);
		expectedCommand.add(ManagedCassandra.FOREGROUND_ARGUMENT_NAME);
		
		managedCassandra.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedCassandra.DEFAULT_CASSANDRA_TARGET_PATH, expectedCommand);
		
		System.clearProperty("CASSANDRA_HOME");
		
	}
	
}
