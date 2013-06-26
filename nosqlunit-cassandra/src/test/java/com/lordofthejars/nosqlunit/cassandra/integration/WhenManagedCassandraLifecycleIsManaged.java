package com.lordofthejars.nosqlunit.cassandra.integration;

import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandra.ManagedCassandraRuleBuilder.newManagedCassandraRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.service.CassandraHost;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.cassandra.ManagedCassandra;
import com.lordofthejars.nosqlunit.cassandra.ManagedCassandraLifecycleManager;
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

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);

		ManagedCassandra managedCassandra = newManagedCassandraRule().build();

		managedCassandra.getManagedCassandraLifecycleManager().setCommandLineExecutor(commandLineExecutor);

		managedCassandra.before();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", CassandraHost.DEFAULT_PORT),
				is(true));

		managedCassandra.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", CassandraHost.DEFAULT_PORT),
				is(false));

		System.clearProperty("CASSANDRA_HOME");

	}


    @Test
	public void managed_cassandra_should_be_registered_and_started_with_custom_parameters() throws Throwable {

		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);

		ManagedCassandra managedCassandra = newManagedCassandraRule().port(9191).build();

		managedCassandra.getManagedCassandraLifecycleManager().setCommandLineExecutor(commandLineExecutor);

		managedCassandra.before();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));

		managedCassandra.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(false));

		System.clearProperty("CASSANDRA_HOME");

	}

    @Test
	public void simulataneous_cassandra_should_start_only_one_instance() throws Throwable {

		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);

		ManagedCassandra managedCassandra = newManagedCassandraRule().port(9191).build();

		managedCassandra.getManagedCassandraLifecycleManager().setCommandLineExecutor(commandLineExecutor);

		managedCassandra.before();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));

		ManagedCassandra managedCassandra2 = newManagedCassandraRule().port(9191).build();

		managedCassandra2.getManagedCassandraLifecycleManager().setCommandLineExecutor(commandLineExecutor);

		managedCassandra2.before();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));
		managedCassandra2.after();

		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(true));

		managedCassandra.after();
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("127.0.0.1", 9191), is(false));

		System.clearProperty("CASSANDRA_HOME");

	}


    @Test
	public void cassandra_should_be_started_using_java() throws Throwable {

		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);

		ManagedCassandra managedCassandra = newManagedCassandraRule().port(9191).build();

		managedCassandra.getManagedCassandraLifecycleManager().setCommandLineExecutor(commandLineExecutor);

		managedCassandra.before();

		managedCassandra.after();

        verify(commandLineExecutor).startProcessInDirectoryAndArguments(eq(ManagedCassandraLifecycleManager.DEFAULT_CASSANDRA_TARGET_PATH),
                anyList());

		System.clearProperty("CASSANDRA_HOME");

	}

    @Test
	public void cassandra_should_be_started_using_java_from_custom_location() throws Throwable {

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(commandLineExecutor.startProcessInDirectoryAndArguments(anyString(), anyList())).thenReturn(mockProcess);

		ManagedCassandra managedCassandra = newManagedCassandraRule().cassandraPath("/opt/cassandra").port(9191)
				.build();

		managedCassandra.getManagedCassandraLifecycleManager().setCommandLineExecutor(commandLineExecutor);

		managedCassandra.before();

		managedCassandra.after();

        verify(commandLineExecutor).startProcessInDirectoryAndArguments(eq(ManagedCassandraLifecycleManager.DEFAULT_CASSANDRA_TARGET_PATH),
                anyList());

	}


	private List<String> getExpectedCommand() {
		List<String> expectedCommand = new ArrayList<String>();

		String javaHome = System.getProperty("java.home");
		expectedCommand.add(javaHome + "/bin/java");
		expectedCommand.add("-ea");
		expectedCommand.add("-javaagent:\"/opt/cassandra/lib/jamm-0.2.5.jar\"");
		expectedCommand.add("-Xms1G");
		expectedCommand.add("-Xmx1G");
		expectedCommand.add("-Dcassandra-foreground=yes");
		expectedCommand.add("-cp");
		expectedCommand
				.add("\"/opt/cassandra/conf\";\"/opt/cassandra/lib/high-scale-lib-1.1.2.jar\";\"/opt/cassandra/lib/avro-1.4.0-fixes.jar\";\"/opt/cassandra/lib/json-simple-1.1.jar\";\"/opt/cassandra/lib/servlet-api-2.5-20081211.jar\";\"/opt/cassandra/lib/compress-lzf-0.8.4.jar\";\"/opt/cassandra/lib/slf4j-log4j12-1.6.1.jar\";\"/opt/cassandra/lib/libthrift-0.7.0.jar\";\"/opt/cassandra/lib/snakeyaml-1.6.jar\";\"/opt/cassandra/lib/snappy-java-1.0.4.1.jar\";\"/opt/cassandra/lib/avro-1.4.0-sources-fixes.jar\";\"/opt/cassandra/lib/guava-r08.jar\";\"/opt/cassandra/lib/apache-cassandra-thrift-1.1.5.jar\";\"/opt/cassandra/lib/commons-lang-2.4.jar\";\"/opt/cassandra/lib/antlr-3.2.jar\";\"/opt/cassandra/lib/jackson-mapper-asl-1.9.2.jar\";\"/opt/cassandra/lib/jackson-core-asl-1.9.2.jar\";\"/opt/cassandra/lib/apache-cassandra-1.1.5.jar\";\"/opt/cassandra/lib/log4j-1.2.16.jar\";\"/opt/cassandra/lib/slf4j-api-1.6.1.jar\";\"/opt/cassandra/lib/jline-0.9.94.jar\";\"/opt/cassandra/lib/snaptree-0.1.jar\";\"/opt/cassandra/lib/jamm-0.2.5.jar\";\"/opt/cassandra/lib/concurrentlinkedhashmap-lru-1.3.jar\";\"/opt/cassandra/lib/commons-cli-1.1.jar\";\"/opt/cassandra/lib/metrics-core-2.0.3.jar\";\"/opt/cassandra/lib/commons-codec-1.2.jar\";\"/opt/cassandra/lib/apache-cassandra-clientutil-1.1.5.jar\"");
		expectedCommand.add("org.apache.cassandra.thrift.CassandraDaemon");
		
		return expectedCommand;

	}

}
