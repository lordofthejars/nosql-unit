package com.lordofthejars.nosqlunit.hbase;

import static com.lordofthejars.nosqlunit.hbase.ManagedHBase.HBaseRuleBuilder.newManagedHBaseServerRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;

public class WhenManagedHBaseLifecycleIsManaged {

	@Mock
	private OperatingSystemResolver operatingSystemResolver;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void managed_hbase_should_be_registered_and_started_with_default_parameters() throws Throwable {
		
		System.setProperty("HBASE_HOME", "/opt/hbase");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		HBaseUtils hBaseUtils = mock(HBaseUtils.class);
		when(hBaseUtils.isConnectionPossible(any(Configuration.class))).thenReturn(true);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedHBase managedHBase = newManagedHBaseServerRule().build();
		
		managedHBase.setCommandLineExecutor(commandLineExecutor);
		managedHBase.setHBaseUtils(hBaseUtils);
		
		managedHBase.before();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", HConstants.DEFAULT_MASTER_PORT),
				is(true));

		managedHBase.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", HConstants.DEFAULT_MASTER_PORT),
				is(false));

		System.clearProperty("HBASE_HOME");
		
	}
	
	@Test
	public void managed_hbase_should_be_registered_and_started_with_custom_parameters() throws Throwable {
		
		System.setProperty("HBASE_HOME", "/opt/hbase");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		HBaseUtils hBaseUtils = mock(HBaseUtils.class);
		when(hBaseUtils.isConnectionPossible(any(Configuration.class))).thenReturn(true);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedHBase managedHBase = newManagedHBaseServerRule().port(111).build();
		
		managedHBase.setCommandLineExecutor(commandLineExecutor);
		managedHBase.setHBaseUtils(hBaseUtils);
		
		managedHBase.before();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 111),
				is(true));

		managedHBase.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 111),
				is(false));

		System.clearProperty("HBASE_HOME");
		
	}
	
	@Test
	public void managed_hbase_should_be_started_from_hbase_home() throws Throwable {
		
		System.setProperty("HBASE_HOME", "/opt/hbase");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		HBaseUtils hBaseUtils = mock(HBaseUtils.class);
		when(hBaseUtils.isConnectionPossible(any(Configuration.class))).thenReturn(true);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedHBase managedHBase = newManagedHBaseServerRule().port(111).build();
		managedHBase.setHBaseUtils(hBaseUtils);
		managedHBase.setCommandLineExecutor(commandLineExecutor);

		managedHBase.before();
		managedHBase.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedHBase.DEFAULT_HBASE_TARGET_PATH,
				getExpectedCommand());
		
		System.clearProperty("HBASE_HOME");
		
	}
	
	@Test
	public void managed_hbase_should_be_stopped() throws Throwable {
		
		System.setProperty("HBASE_HOME", "/opt/hbase");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		HBaseUtils hBaseUtils = mock(HBaseUtils.class);
		when(hBaseUtils.isConnectionPossible(any(Configuration.class))).thenReturn(true);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedHBase managedHBase = newManagedHBaseServerRule().port(111).build();
		
		managedHBase.setCommandLineExecutor(commandLineExecutor);
		managedHBase.setHBaseUtils(hBaseUtils);
		
		managedHBase.before();
		managedHBase.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedHBase.DEFAULT_HBASE_TARGET_PATH,
				getStopExpectedCommand());
		
		System.clearProperty("HBASE_HOME");
		
	}
	
	@Test
	public void managed_hbase_should_be_started_from_custom_location() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		HBaseUtils hBaseUtils = mock(HBaseUtils.class);
		when(hBaseUtils.isConnectionPossible(any(Configuration.class))).thenReturn(true);
		
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedHBase managedHBase = newManagedHBaseServerRule().hBasePath("/opt/hbase").build();
		
		managedHBase.setCommandLineExecutor(commandLineExecutor);
		managedHBase.setHBaseUtils(hBaseUtils);
		managedHBase.before();
		managedHBase.after();
		
		verify(commandLineExecutor).startProcessInDirectoryAndArguments(ManagedHBase.DEFAULT_HBASE_TARGET_PATH,
				getExpectedCommand());
		
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void managed_hbase_should_throw_an_exception_in_windows_systems() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.WINDOWS_2003);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);
		HBaseUtils hBaseUtils = mock(HBaseUtils.class);
		when(hBaseUtils.isConnectionPossible(any(Configuration.class))).thenReturn(true);
		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);
		
		ManagedHBase managedHBase = newManagedHBaseServerRule().hBasePath("/opt/hbase").build();
		managedHBase.setHBaseUtils(hBaseUtils);
		managedHBase.setCommandLineExecutor(commandLineExecutor);
		managedHBase.setOperatingSystemResolver(operatingSystemResolver);

		managedHBase.before();
		
		
	}
	
	private List<String> getStopExpectedCommand() {
		
		List<String> expectedCommand = new ArrayList<String>();
		
		expectedCommand.add("/opt/hbase/bin/stop-hbase.sh");
		
		return expectedCommand;
		
	}
	
	private List<String> getExpectedCommand() {
	
		List<String> expectedCommand = new ArrayList<String>();
		
		expectedCommand.add("/opt/hbase/bin/start-hbase.sh");
		
		return expectedCommand;
		
	}
}
