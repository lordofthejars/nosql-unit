package com.lordofthejars.nosqlunit.infinispan;

import static com.lordofthejars.nosqlunit.infinispan.ManagedInfinispan.ManagedInfinispanRuleBuilder.newManagedInfinispanRule;
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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;

public class WhenManagedInfinispanLifecycleIsManaged {

	@Mock
	private OperatingSystemResolver operatingSystemResolver;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void managed_infinispan_should_be_registered_and_started_with_default_parameters() throws Throwable {
		
		System.setProperty("INFINISPAN_HOME", "/opt/infinispan-5.1.6");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedInfinispan managedInfinispan = newManagedInfinispanRule().build();
		
		managedInfinispan.managedInfinispanLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedInfinispan.managedInfinispanLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedInfinispan.before();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", ManagedInfinispanLifecycleManager.DEFAULT_PORT), is(true));
		
		managedInfinispan.after();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", ManagedInfinispanLifecycleManager.DEFAULT_PORT), is(false));
		
		System.clearProperty("INFINISPAN_HOME");
		
	}
	
	@Test
	public void infinispan_should_be_started_in_Linux() throws Throwable {
		
		System.setProperty("INFINISPAN_HOME", "/opt/infinispan-5.1.6");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedInfinispan managedInfinispan = newManagedInfinispanRule().build();
		
		managedInfinispan.managedInfinispanLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedInfinispan.managedInfinispanLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedInfinispan.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/infinispan-5.1.6"+File.separatorChar+ManagedInfinispanLifecycleManager.INFINISPAN_BINARY_DIRECTORY+File.separatorChar+ManagedInfinispanLifecycleManager.INFINISPAN_EXECUTABLE_X);
		expectedCommand.add(ManagedInfinispanLifecycleManager.PORT_COMMAND_LINE_ARGUMENT_NAME);
		expectedCommand.add(Integer.toString(ManagedInfinispanLifecycleManager.DEFAULT_PORT));
		expectedCommand.add(ManagedInfinispanLifecycleManager.PROTOCOL_COMMAND_LINE_ARGUMENT_NAME);
		expectedCommand.add(ManagedInfinispanLifecycleManager.PROTOCOL_COMMAND_LINE_DEFAULT_VALUE);
		
		managedInfinispan.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedInfinispanLifecycleManager.DEFAULT_INFINISPAN_TARGET_PATH, expectedCommand);
		
		System.clearProperty("INFINISPAN_HOME");
		
	}
	
	
	@Test
	public void infinispan_should_be_started_in_Linux_with_custom_location() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		System.clearProperty("INFINISPAN_HOME");
		ManagedInfinispan managedInfinispan = newManagedInfinispanRule().infinispanPath("/opt/infinispan-5.1.6").build();
		
		managedInfinispan.managedInfinispanLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedInfinispan.managedInfinispanLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedInfinispan.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/infinispan-5.1.6"+File.separatorChar+ManagedInfinispanLifecycleManager.INFINISPAN_BINARY_DIRECTORY+File.separatorChar+ManagedInfinispanLifecycleManager.INFINISPAN_EXECUTABLE_X);
		expectedCommand.add(ManagedInfinispanLifecycleManager.PORT_COMMAND_LINE_ARGUMENT_NAME);
		expectedCommand.add(Integer.toString(ManagedInfinispanLifecycleManager.DEFAULT_PORT));
		expectedCommand.add(ManagedInfinispanLifecycleManager.PROTOCOL_COMMAND_LINE_ARGUMENT_NAME);
		expectedCommand.add(ManagedInfinispanLifecycleManager.PROTOCOL_COMMAND_LINE_DEFAULT_VALUE);
		
		managedInfinispan.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedInfinispanLifecycleManager.DEFAULT_INFINISPAN_TARGET_PATH, expectedCommand);
	}
	
	@Test
	public void infinispan_should_be_started_in_MacOsX() throws Throwable {
		
		System.setProperty("INFINISPAN_HOME", "/opt/infinispan-5.1.6");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.MAC_OSX);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedInfinispan managedInfinispan = newManagedInfinispanRule().build();
		
		managedInfinispan.managedInfinispanLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedInfinispan.managedInfinispanLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedInfinispan.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/infinispan-5.1.6"+File.separatorChar+ManagedInfinispanLifecycleManager.INFINISPAN_BINARY_DIRECTORY+File.separatorChar+ManagedInfinispanLifecycleManager.INFINISPAN_EXECUTABLE_X);
		expectedCommand.add(ManagedInfinispanLifecycleManager.PORT_COMMAND_LINE_ARGUMENT_NAME);
		expectedCommand.add(Integer.toString(ManagedInfinispanLifecycleManager.DEFAULT_PORT));
		expectedCommand.add(ManagedInfinispanLifecycleManager.PROTOCOL_COMMAND_LINE_ARGUMENT_NAME);
		expectedCommand.add(ManagedInfinispanLifecycleManager.PROTOCOL_COMMAND_LINE_DEFAULT_VALUE);
		
		managedInfinispan.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedInfinispanLifecycleManager.DEFAULT_INFINISPAN_TARGET_PATH, expectedCommand);
		
		System.clearProperty("INFINISPAN_HOME");
		
	}
	
	@Test
	public void infinispan_should_be_started_in_Windows() throws Throwable {
		
		System.setProperty("INFINISPAN_HOME", "C:/infinispan-5.1.6");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.WINDOWS_7);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedInfinispan managedInfinispan = newManagedInfinispanRule().build();
		
		managedInfinispan.managedInfinispanLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedInfinispan.managedInfinispanLifecycleManager.setOperatingSystemResolver(operatingSystemResolver);
		
		managedInfinispan.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("C:/infinispan-5.1.6"+File.separatorChar+ManagedInfinispanLifecycleManager.INFINISPAN_BINARY_DIRECTORY+File.separatorChar+ManagedInfinispanLifecycleManager.INFINISPAN_EXECUTABLE_W);
		expectedCommand.add(ManagedInfinispanLifecycleManager.PORT_COMMAND_LINE_ARGUMENT_NAME);
		expectedCommand.add(Integer.toString(ManagedInfinispanLifecycleManager.DEFAULT_PORT));
		expectedCommand.add(ManagedInfinispanLifecycleManager.PROTOCOL_COMMAND_LINE_ARGUMENT_NAME);
		expectedCommand.add(ManagedInfinispanLifecycleManager.PROTOCOL_COMMAND_LINE_DEFAULT_VALUE);
		
		managedInfinispan.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedInfinispanLifecycleManager.DEFAULT_INFINISPAN_TARGET_PATH, expectedCommand);
		
		System.clearProperty("INFINISPAN_HOME");
		
	}
	
}
