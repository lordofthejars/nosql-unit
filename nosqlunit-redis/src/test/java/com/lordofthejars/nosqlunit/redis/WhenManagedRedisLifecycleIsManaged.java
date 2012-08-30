package com.lordofthejars.nosqlunit.redis;

import static com.lordofthejars.nosqlunit.redis.ManagedRedis.ManagedRedisRuleBuilder.newManagedRedisRule;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyList;
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
import com.lordofthejars.nosqlunit.redis.ManagedRedis;

public class WhenManagedRedisLifecycleIsManaged {

	
	@Mock
	private OperatingSystemResolver operatingSystemResolver;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void managed_redis_should_be_registered_and_started_with_default_parameters() throws Throwable {
		
		System.setProperty("REDIS_HOME", "/opt/redis-2.4.16");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedRedis managedRedis = newManagedRedisRule().build();
		
		managedRedis.setCommandLineExecutor(commandLineExecutor);
		managedRedis.setOperatingSystemResolver(operatingSystemResolver);
		
		managedRedis.before();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", ManagedRedis.DEFAULT_PORT), is(true));

		managedRedis.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", ManagedRedis.DEFAULT_PORT), is(false));
		
		System.clearProperty("REDIS_HOME");
		
	}
	
	@Test
	public void managed_redis_should_be_registered_and_started_with_custom_parameters() throws Throwable {
		
		System.setProperty("REDIS_HOME", "/opt/redis-2.4.16");
		
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedRedis managedRedis = newManagedRedisRule().port(9191).build();
		
		managedRedis.setCommandLineExecutor(commandLineExecutor);
		managedRedis.setOperatingSystemResolver(operatingSystemResolver);
		
		managedRedis.before();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));

		managedRedis.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(false));
		
		
		System.clearProperty("REDIS_HOME");
		
	}
	
	@Test
	public void redis_should_be_started_in_Linux() throws Throwable {
		
		System.setProperty("REDIS_HOME", "/opt/redis-2.4.16");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedRedis managedRedis = newManagedRedisRule().build();
		
		managedRedis.setCommandLineExecutor(commandLineExecutor);
		managedRedis.setOperatingSystemResolver(operatingSystemResolver);
		
		managedRedis.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/redis-2.4.16"+File.separatorChar+ManagedRedis.REDIS_BINARY_DIRECTORY+File.separatorChar+ManagedRedis.REDIS_EXECUTABLE_X);
		
		managedRedis.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedRedis.DEFAULT_REDIS_TARGET_PATH, expectedCommand);
		
		System.clearProperty("REDIS_HOME");
		
	}
	
	@Test
	public void redis_should_be_started_in_Linux_with_custom_location() throws Throwable {
		
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedRedis managedRedis = newManagedRedisRule().redisPath("/opt/redis-2.4.16").build();
		
		managedRedis.setCommandLineExecutor(commandLineExecutor);
		managedRedis.setOperatingSystemResolver(operatingSystemResolver);
		
		managedRedis.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/redis-2.4.16"+File.separatorChar+ManagedRedis.REDIS_BINARY_DIRECTORY+File.separatorChar+ManagedRedis.REDIS_EXECUTABLE_X);
		
		managedRedis.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedRedis.DEFAULT_REDIS_TARGET_PATH, expectedCommand);
		
	}
	
	@Test
	public void redis_should_be_started_in_MacOsX() throws Throwable {
		
		System.setProperty("REDIS_HOME", "/opt/redis-2.4.16");
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.MAC_OSX);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedRedis managedRedis = newManagedRedisRule().build();
		
		managedRedis.setCommandLineExecutor(commandLineExecutor);
		managedRedis.setOperatingSystemResolver(operatingSystemResolver);
		
		managedRedis.before();
		
		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add("/opt/redis-2.4.16"+File.separatorChar+ManagedRedis.REDIS_BINARY_DIRECTORY+File.separatorChar+ManagedRedis.REDIS_EXECUTABLE_X);
		
		managedRedis.after();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				ManagedRedis.DEFAULT_REDIS_TARGET_PATH, expectedCommand);
		
		System.clearProperty("REDIS_HOME");
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void redis_should_not_be_started_in_Windows() throws Throwable {
		
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.WINDOWS_7);
		
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedRedis managedRedis = newManagedRedisRule().redisPath("C:\\....").build();
		
		managedRedis.setCommandLineExecutor(commandLineExecutor);
		managedRedis.setOperatingSystemResolver(operatingSystemResolver);
		
		managedRedis.before();
		managedRedis.after();
		
	}
	
}
