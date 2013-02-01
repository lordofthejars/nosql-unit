package com.lordofthejars.nosqlunit.mongodb.shard;

import static com.lordofthejars.nosqlunit.mongodb.shard.ManagedMongosLifecycleManagerBuilder.newManagedMongosLifecycle;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.mongodb.MongoDbLowLevelOps;
import com.mongodb.DBPort;

public class WhenMongosIsRemoteManaged {

	private static final String MONGODB_LOCATION = "/Users/alex/Applications/mongodb-osx-x86_64-2.0.5";

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Mock
	private OperatingSystemResolver operatingSystemResolver;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void mongodb_should_start_mongodb_instance_in_Linux()
			throws Throwable {

		MongoDbLowLevelOps mongoDbChecker = mock(MongoDbLowLevelOps.class);
		when(
				mongoDbChecker.assertThatConnectionIsPossible(anyString(),
						anyInt())).thenReturn(true);
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList());

		ManagedMongosLifecycleManager managedMongosLifecycleManager = newManagedMongosLifecycle()
				.mongosPath(MONGODB_LOCATION).configServer("localhost", 27020)
				.configServer("localhost", 27021).get();

		managedMongosLifecycleManager
				.setCommandLineExecutor(commandLineExecutor);
		managedMongosLifecycleManager
				.setOperatingSystemResolver(operatingSystemResolver);
		managedMongosLifecycleManager.setMongoDbLowLevelOps(mongoDbChecker);

		managedMongosLifecycleManager.startEngine();

		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add(MONGODB_LOCATION + File.separatorChar
				+ ManagedMongosLifecycleManager.MONGODB_BINARY_DIRECTORY
				+ File.separatorChar
				+ ManagedMongosLifecycleManager.MONGOS_EXECUTABLE_X);
		expectedCommand.add(ManagedMongosLifecycleManager.PORT_ARGUMENT_NAME);
		expectedCommand.add(DBPort.PORT + "");
		expectedCommand
				.add(ManagedMongosLifecycleManager.LOGPATH_ARGUMENT_NAME);
		expectedCommand
				.add(ManagedMongosLifecycleManager.DEFAULT_MONGO_LOGPATH);
		expectedCommand
				.add(ManagedMongosLifecycleManager.CHUNK_SIZE_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongosLifecycleManager.DEFAULT_CHUNK_SIZE
				+ "");
		expectedCommand
				.add(ManagedMongosLifecycleManager.CONFIG_DB_ARGUMENT_NAME);
		expectedCommand.add("localhost:27020, localhost:27021");

		managedMongosLifecycleManager.stopEngine();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				"target/mongo-temp", expectedCommand);

	}
	
	@Test
	public void mongodb_should_start_mongodb_instance_in_Windows()
			throws Throwable {

		MongoDbLowLevelOps mongoDbChecker = mock(MongoDbLowLevelOps.class);
		when(
				mongoDbChecker.assertThatConnectionIsPossible(anyString(),
						anyInt())).thenReturn(true);
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.WINDOWS_2008);

		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList());

		ManagedMongosLifecycleManager managedMongosLifecycleManager = newManagedMongosLifecycle()
				.mongosPath(MONGODB_LOCATION).configServer("localhost", 27020)
				.configServer("localhost", 27021).get();

		managedMongosLifecycleManager
				.setCommandLineExecutor(commandLineExecutor);
		managedMongosLifecycleManager
				.setOperatingSystemResolver(operatingSystemResolver);
		managedMongosLifecycleManager.setMongoDbLowLevelOps(mongoDbChecker);

		managedMongosLifecycleManager.startEngine();

		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add(MONGODB_LOCATION + File.separatorChar
				+ ManagedMongosLifecycleManager.MONGODB_BINARY_DIRECTORY
				+ File.separatorChar
				+ ManagedMongosLifecycleManager.MONGOS_EXECUTABLE_W);
		expectedCommand.add(ManagedMongosLifecycleManager.PORT_ARGUMENT_NAME);
		expectedCommand.add(DBPort.PORT + "");
		expectedCommand
				.add(ManagedMongosLifecycleManager.LOGPATH_ARGUMENT_NAME);
		expectedCommand
				.add(ManagedMongosLifecycleManager.DEFAULT_MONGO_LOGPATH);
		expectedCommand
				.add(ManagedMongosLifecycleManager.CHUNK_SIZE_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongosLifecycleManager.DEFAULT_CHUNK_SIZE
				+ "");
		expectedCommand
				.add(ManagedMongosLifecycleManager.CONFIG_DB_ARGUMENT_NAME);
		expectedCommand.add("localhost:27020, localhost:27021");

		managedMongosLifecycleManager.stopEngine();

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				"target/mongo-temp", expectedCommand);

	}

}
