package com.lordofthejars.nosqlunit.couchdb;

import static com.lordofthejars.nosqlunit.couchdb.ManagedCouchDb.ManagedCouchDbRuleBuilder.newManagedCouchDbRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

public class WhenManagedCouchDbLifecycleIsManaged {

	@Mock
	private OperatingSystemResolver operatingSystemResolver;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void managed_couchdb_should_be_registered_and_started_with_default_parameters() throws Throwable {
		
		System.setProperty("COUCHDB_HOME", "/usr/local");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCouchDb managedCouchDb = newManagedCouchDbRule().build();
		
		managedCouchDb.managedCouchDbLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		
		managedCouchDb.before();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(ManagedCouchDbLifecycleManager.LOCALHOST, ManagedCouchDbLifecycleManager.DEFAULT_PORT),
				is(true));

		managedCouchDb.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(ManagedCouchDbLifecycleManager.LOCALHOST, ManagedCouchDbLifecycleManager.DEFAULT_PORT),
				is(false));

		System.clearProperty("COUCHDB_HOME");
		
	}
	
	@Test
	public void managed_couchdb_should_be_registered_and_started_with_custom_parameters() throws Throwable {
		
		System.setProperty("COUCHDB_HOME", "/usr/local");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCouchDb managedCouchDb = newManagedCouchDbRule().port(111).build();
		
		managedCouchDb.managedCouchDbLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		
		managedCouchDb.before();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(ManagedCouchDbLifecycleManager.LOCALHOST, 111),
				is(true));

		managedCouchDb.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(ManagedCouchDbLifecycleManager.LOCALHOST, 111),
				is(false));

		System.clearProperty("COUCHDB_HOME");
		
	}
	
	@Test
	public void managed_couchdb_should_be_started_from_couchdb_home() throws Throwable {
		
		System.setProperty("COUCHDB_HOME", "/usr/local");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCouchDb managedCouchDb = newManagedCouchDbRule().port(111).build();
		managedCouchDb.managedCouchDbLifecycleManager.setCommandLineExecutor(commandLineExecutor);

		managedCouchDb.before();
		managedCouchDb.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedCouchDbLifecycleManager.DEFAULT_COUCHDB_TARGET_PATH,
				getExpectedXCommand());
		
		System.clearProperty("COUCHDB_HOME");
		
	}
	
	@Test
	public void managed_couchdb_should_be_stopped() throws Throwable {
		
		System.setProperty("COUCHDB_HOME", "/usr/local");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCouchDb managedCouchDb = newManagedCouchDbRule().build();
		
		managedCouchDb.managedCouchDbLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		
		managedCouchDb.before();
		managedCouchDb.after();
		
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered(ManagedCouchDbLifecycleManager.LOCALHOST, 111),
				is(false));
		
		System.clearProperty("COUCHDB_HOME");
		
	}
	
	@Test
	public void managed_couchdb_should_be_started_from_custom_location() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCouchDb managedCouchDb = newManagedCouchDbRule().couchDbPath("/usr/local").build();
		
		managedCouchDb.managedCouchDbLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedCouchDb.before();
		managedCouchDb.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedCouchDbLifecycleManager.DEFAULT_COUCHDB_TARGET_PATH,
				getExpectedXCommand());
		
		
	}
	
	public void managed_couchdb_should_start_from_windows_systems() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.WINDOWS_7);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedCouchDb managedCouchDb = newManagedCouchDbRule().couchDbPath("/usr/local").build();
		
		managedCouchDb.managedCouchDbLifecycleManager.setCommandLineExecutor(commandLineExecutor);
		managedCouchDb.before();
		managedCouchDb.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedCouchDbLifecycleManager.DEFAULT_COUCHDB_TARGET_PATH,
				getExpectedWindowsCommand());
		
		
	}
	
	private List<String> getExpectedWindowsCommand() {
		
		List<String> expectedCommand = new ArrayList<String>();
		
		expectedCommand.add("/usr/local/bin/couchdb.bat");
		
		return expectedCommand;
		
	}
	
	private List<String> getExpectedXCommand() {
	
		List<String> expectedCommand = new ArrayList<String>();
		
		expectedCommand.add("/usr/local/bin/couchdb");
		
		return expectedCommand;
		
	}
	
}
