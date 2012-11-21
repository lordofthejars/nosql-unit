package com.lordofthejars.nosqlunit.neo4j;

import static com.lordofthejars.nosqlunit.neo4j.ManagedNeoServer.Neo4jServerRuleBuilder.newManagedNeo4jServerRule;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;

public class WhenNeo4jIsRemoteManaged {

	private static final String NEO4J_LOCATION = "/Users/alex/Applications/neo4j-community-1.7.2/";


	@Mock
	private OperatingSystemResolver operatingSystemResolver;


	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void neo4j_should_start_and_stop_from_neo4j_home() throws Throwable {
		
		System.setProperty("NEO4J_HOME", "/opt/neo4j-community-1.7.2");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.WINDOWS_7);
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList()).thenReturn(Arrays.asList("Server available")).thenReturn(
						Collections.<String> emptyList()).thenReturn(
								Collections.<String> emptyList());

		ManagedNeoServer managedNeoServer = newManagedNeo4jServerRule().build();
		managedNeoServer.managedNeoServerLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedNeoServer.managedNeoServerLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedNeoServer.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/neo4j-community-1.7.2"+File.separatorChar+ManagedNeoServerLifecycleManager.NEO4J_BINARY_DIRECTORY+File.separatorChar+ManagedNeoServerLifecycleManager.NEO4J_EXECUTABLE_W);
		expectedCommand.add("start");
		
		managedNeoServer.after();
	
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedNeoServerLifecycleManager.DEFAULT_NEO4J_TARGET_PATH, expectedCommand);
		
		System.clearProperty("NEO4J_HOME");
		
	}
	
	@Test
	public void neo4j_should_be_started_in_Windows() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.WINDOWS_7);
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList()).thenReturn(Arrays.asList("Server available")).thenReturn(
						Collections.<String> emptyList()).thenReturn(
								Collections.<String> emptyList());

		ManagedNeoServer managedNeoServer = newManagedNeo4jServerRule().neo4jPath(NEO4J_LOCATION).build();
		managedNeoServer.managedNeoServerLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedNeoServer.managedNeoServerLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedNeoServer.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add(NEO4J_LOCATION+File.separatorChar+ManagedNeoServerLifecycleManager.NEO4J_BINARY_DIRECTORY+File.separatorChar+ManagedNeoServerLifecycleManager.NEO4J_EXECUTABLE_W);
		expectedCommand.add("start");
		
		managedNeoServer.after();
	
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedNeoServerLifecycleManager.DEFAULT_NEO4J_TARGET_PATH, expectedCommand);
		
	}
	
	@Test
	public void neo4j_should_be_started_in_Linux() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList()).thenReturn(Arrays.asList("Server available")).thenReturn(
						Collections.<String> emptyList()).thenReturn(
								Collections.<String> emptyList());

		ManagedNeoServer managedNeoServer = newManagedNeo4jServerRule().neo4jPath(NEO4J_LOCATION).build();
		managedNeoServer.managedNeoServerLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedNeoServer.managedNeoServerLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedNeoServer.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add(NEO4J_LOCATION+File.separatorChar+ManagedNeoServerLifecycleManager.NEO4J_BINARY_DIRECTORY+File.separatorChar+ManagedNeoServerLifecycleManager.NEO4J_EXECUTABLE_X);
		expectedCommand.add("start");
		
		managedNeoServer.after();
	
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedNeoServerLifecycleManager.DEFAULT_NEO4J_TARGET_PATH, expectedCommand);
		
	}
	
	@Test
	public void neo4j_should_be_started_in_MacOS() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.MAC_OSX);
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList()).thenReturn(Arrays.asList("Server available")).thenReturn(
						Collections.<String> emptyList()).thenReturn(
								Collections.<String> emptyList());

		ManagedNeoServer managedNeoServer = newManagedNeo4jServerRule().neo4jPath(NEO4J_LOCATION).build();
		managedNeoServer.managedNeoServerLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedNeoServer.managedNeoServerLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedNeoServer.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add(NEO4J_LOCATION+File.separatorChar+ManagedNeoServerLifecycleManager.NEO4J_BINARY_DIRECTORY+File.separatorChar+ManagedNeoServerLifecycleManager.NEO4J_EXECUTABLE_X);
		expectedCommand.add("start");
		
		managedNeoServer.after();
	
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedNeoServerLifecycleManager.DEFAULT_NEO4J_TARGET_PATH, expectedCommand);
		
	}
	
}
