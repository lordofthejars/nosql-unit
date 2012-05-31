package com.lordofthejars.nosqlunit.mongodb;

import static com.lordofthejars.nosqlunit.mongodb.ManagedMongoDb.MongoServerRuleBuilder.newManagedMongoDbRule;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
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

public class WhenMongoDbIsRemoteManaged {

	private static final String MONGODB_LOCATION = "/Users/alex/Applications/mongodb-osx-x86_64-2.0.5/";

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Mock
	private OperatingSystemResolver operatingSystemResolver;

	@Mock
	MongoDbLowLevelOps mongoDbChecker;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void mongodb_data_directory_should_be_deleted_when_server_is_stopped() throws IOException {

		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.MAC_OSX);
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		MongoDbLowLevelOps mongoDbChecker = mock(MongoDbLowLevelOps.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList());

		File targetPath = temporaryFolder.newFolder();
		createDbPathDirectory(targetPath);
		
		ManagedMongoDb managedMongoDb = newManagedMongoDbRule()
				.targetPath(targetPath.toString()).mongodPath(MONGODB_LOCATION)
				.build();

		managedMongoDb.setCommandLineExecutor(commandLineExecutor);
		managedMongoDb.setOperatingSystemResolver(operatingSystemResolver);
		managedMongoDb.setMongoDbLowLevelOps(mongoDbChecker);
		
		managedMongoDb.after();
		File dbPath = new File(targetPath.toString() + File.separatorChar + ManagedMongoDb.DEFAULT_MONGO_DBPATH);
		assertThat(dbPath.exists(), is(false));
		

	}

	private File createDbPathDirectory(File targetPath) {
		File dataDirectory = new File(targetPath.toString() + File.separatorChar + ManagedMongoDb.DEFAULT_MONGO_DBPATH);
		dataDirectory.mkdirs();
		return dataDirectory;
	}

	@Test
	public void mongodb_should_start_mongodb_instance_in_Windows() throws Throwable {

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.WINDOWS_7);
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		MongoDbLowLevelOps mongoDbChecker = mock(MongoDbLowLevelOps.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList());

		File targetPath = temporaryFolder.newFolder();

		ManagedMongoDb managedMongoDb = newManagedMongoDbRule()
				.targetPath(targetPath.toString()).mongodPath(MONGODB_LOCATION)
				.appendCommandLineArguments("myArgument", "myValue").build();

		managedMongoDb.setCommandLineExecutor(commandLineExecutor);
		managedMongoDb.setOperatingSystemResolver(operatingSystemResolver);
		managedMongoDb.setMongoDbLowLevelOps(mongoDbChecker);

		managedMongoDb.before();

		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add(MONGODB_LOCATION + File.separatorChar
				+ ManagedMongoDb.MONGODB_BINARY_DIRECTORY + File.separatorChar
				+ ManagedMongoDb.MONGODB_EXECUTABLE_W);
		expectedCommand.add(ManagedMongoDb.DBPATH_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.DEFAULT_MONGO_DBPATH);
		expectedCommand.add(ManagedMongoDb.FORK_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.LOGPATH_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.DEFAULT_MONGO_LOGPATH);
		expectedCommand.add("myArgument");
		expectedCommand.add("myValue");

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				targetPath.toString(), expectedCommand);

	}
	
	@Test
	public void mongodb_should_start_mongodb_instance_in_Linux() throws Throwable {

		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.LINUX_OS);
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		MongoDbLowLevelOps mongoDbChecker = mock(MongoDbLowLevelOps.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList());

		File targetPath = temporaryFolder.newFolder();

		ManagedMongoDb managedMongoDb = newManagedMongoDbRule()
				.targetPath(targetPath.toString()).mongodPath(MONGODB_LOCATION)
				.appendCommandLineArguments("myArgument", "myValue").build();

		managedMongoDb.setCommandLineExecutor(commandLineExecutor);
		managedMongoDb.setOperatingSystemResolver(operatingSystemResolver);
		managedMongoDb.setMongoDbLowLevelOps(mongoDbChecker);

		managedMongoDb.before();

		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add(MONGODB_LOCATION + File.separatorChar
				+ ManagedMongoDb.MONGODB_BINARY_DIRECTORY + File.separatorChar
				+ ManagedMongoDb.MONGODB_EXECUTABLE_X);
		expectedCommand.add(ManagedMongoDb.DBPATH_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.DEFAULT_MONGO_DBPATH);
		expectedCommand.add(ManagedMongoDb.FORK_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.LOGPATH_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.DEFAULT_MONGO_LOGPATH);
		expectedCommand.add("myArgument");
		expectedCommand.add("myValue");

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				targetPath.toString(), expectedCommand);

	}
	
	@Test
	public void mongodb_should_start_mongodb_instance_in_Mac() throws Throwable {
		
		when(operatingSystemResolver.currentOperatingSystem()).thenReturn(
				OperatingSystem.MAC_OSX);
		
		CommandLineExecutor commandLineExecutor = mock(CommandLineExecutor.class);

		MongoDbLowLevelOps mongoDbChecker = mock(MongoDbLowLevelOps.class);

		Process mockProcess = mock(Process.class);
		when(mockProcess.exitValue()).thenReturn(0);

		when(
				commandLineExecutor.startProcessInDirectoryAndArguments(
						anyString(), anyList())).thenReturn(mockProcess);
		when(commandLineExecutor.getConsoleOutput(mockProcess)).thenReturn(
				Collections.<String> emptyList());

		File targetPath = temporaryFolder.newFolder();

		ManagedMongoDb managedMongoDb = newManagedMongoDbRule()
				.targetPath(targetPath.toString()).mongodPath(MONGODB_LOCATION)
				.appendCommandLineArguments("myArgument", "myValue").build();

		managedMongoDb.setCommandLineExecutor(commandLineExecutor);
		managedMongoDb.setOperatingSystemResolver(operatingSystemResolver);
		managedMongoDb.setMongoDbLowLevelOps(mongoDbChecker);

		managedMongoDb.before();

		List<String> expectedCommand = new ArrayList<String>();
		expectedCommand.add(MONGODB_LOCATION + File.separatorChar
				+ ManagedMongoDb.MONGODB_BINARY_DIRECTORY + File.separatorChar
				+ ManagedMongoDb.MONGODB_EXECUTABLE_X);
		expectedCommand.add(ManagedMongoDb.DBPATH_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.DEFAULT_MONGO_DBPATH);
		expectedCommand.add(ManagedMongoDb.FORK_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.LOGPATH_ARGUMENT_NAME);
		expectedCommand.add(ManagedMongoDb.DEFAULT_MONGO_LOGPATH);
		expectedCommand.add("myArgument");
		expectedCommand.add("myValue");

		verify(commandLineExecutor).startProcessInDirectoryAndArguments(
				targetPath.toString(), expectedCommand);

	}

}
