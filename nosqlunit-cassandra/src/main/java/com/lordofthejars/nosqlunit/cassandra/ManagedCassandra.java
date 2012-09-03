package com.lordofthejars.nosqlunit.cassandra;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import me.prettyprint.cassandra.service.CassandraHost;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.core.OsNameSystemPropertyOperatingSystemResolver;

public class ManagedCassandra extends AbstractLifecycleManager {

	static Process pwd;
	
	private static final String LOCALHOST = "127.0.0.1";

	protected static final String FOREGROUND_ARGUMENT_NAME = "-f";
	
	protected static final String DEFAULT_CASSANDRA_TARGET_PATH = "target" + File.separatorChar + "cassandra-temp";
	protected static final String CASSANDRA_BINARY_DIRECTORY = "bin";

	protected static final String CASSANDRA_EXECUTABLE_X = "cassandra";
	protected static final String CASSANDRA_EXECUTABLE_W = "cassandra.bat";

	
	private String targetPath = DEFAULT_CASSANDRA_TARGET_PATH;
	private String cassandraPath = System.getProperty("CASSANDRA_HOME");
	private int port = CassandraHost.DEFAULT_PORT;

	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();
	private OperatingSystemResolver operatingSystemResolver = new OsNameSystemPropertyOperatingSystemResolver();

	private ManagedCassandra() {
		super();
	}

	public static class ManagedCassandraRuleBuilder {

		private ManagedCassandra managedCassandra;

		private ManagedCassandraRuleBuilder() {
			this.managedCassandra = new ManagedCassandra();
		}

		public static ManagedCassandraRuleBuilder newManagedCassandraRule() {
			return new ManagedCassandraRuleBuilder();
		}

		public ManagedCassandraRuleBuilder port(int port) {
			this.managedCassandra.setPort(port);
			return this;
		}

		public ManagedCassandraRuleBuilder targetPath(String targetPath) {
			this.managedCassandra.setTargetPath(targetPath);
			return this;
		}
		
		public ManagedCassandraRuleBuilder cassandraPath(String cassandraPath) {
			this.managedCassandra.setCassandraPath(cassandraPath);
			return this;
		}
		
		public ManagedCassandraRuleBuilder appendCommandLineArguments(String argumentName, String argumentValue) {
			this.managedCassandra.addExtraCommandLineArgument(argumentName, argumentValue);
			return this;
		}

		public ManagedCassandraRuleBuilder appendSingleCommandLineArguments(String argument) {
			this.managedCassandra.addSingleCommandLineArgument(argument);
			return this;
		}
		
		public ManagedCassandra build() {
			if(this.managedCassandra.getCassandraPath() == null) {
				throw new IllegalArgumentException("Cassandra Path cannot be null.");
			}
			
			return this.managedCassandra;
		}

	}
	
	@Override
	protected String getHost() {
		return LOCALHOST;
	}


	@Override
	protected int getPort() {
		return port;
	}


	@Override
	protected void doStart() throws Throwable {

		File targetPathDirectory = ensureTargetPathDoesNotExitsAndReturnCompositePath();

		if (targetPathDirectory.mkdirs()) {
			startCassandraAsDaemon();
		} else {
			throw new IllegalStateException("Target Path " + targetPathDirectory + " could not be created.");
		}
	}


	private void startCassandraAsDaemon() throws AssertionError {
		final CountDownLatch startupLatch = new CountDownLatch(1);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					startCassandraProcess();
					startupLatch.countDown();
				} catch (InterruptedException e) {
					throw new IllegalStateException(e);
				}
			}
		}).start();
		
		try {
			startupLatch.await(10, SECONDS);
		} catch (InterruptedException e) {
			throw new AssertionError(e);
		}
	}


	@Override
	protected void doStop() {
		try {
			stopCassandra();
		}finally {
			ensureTargetPathDoesNotExitsAndReturnCompositePath();
		}
	}

	private List<String> startCassandraProcess() throws InterruptedException {

		try {
			pwd = startProcess();
			pwd.waitFor();
			
			if (pwd.exitValue() != 0) {
				List<String> consoleOutput = getConsoleOutput(pwd);
				System.out.println(consoleOutput);
			}
			return null;
		} catch (IOException e) {
			throw new IllegalStateException("Cassandra [" + cassandraPath + " at port " + port
					+ "] could not be started. Next console message was thrown: ");
		}
	}

	private Process startProcess() throws IOException {
		return this.commandLineExecutor.startProcessInDirectoryAndArguments(targetPath,
				buildOperationSystemProgramAndArguments());
	}

	private List<String> getConsoleOutput(Process pwd) throws IOException {
		return this.commandLineExecutor.getConsoleOutput(pwd);
	}
	
	private List<String> buildOperationSystemProgramAndArguments() {

		List<String> programAndArguments = new ArrayList<String>();

		programAndArguments.add(getExecutablePath());
		programAndArguments.add("start");
		programAndArguments.add(FOREGROUND_ARGUMENT_NAME);
		
		for (String argument : this.singleCommandArguments) {
			programAndArguments.add(argument);
		}

		for (String argumentName : this.extraCommandArguments.keySet()) {
			programAndArguments.add(argumentName);
			programAndArguments.add(this.extraCommandArguments
					.get(argumentName));
		}

		return programAndArguments;

	}

	private String getExecutablePath() {
		return this.cassandraPath + File.separatorChar + CASSANDRA_BINARY_DIRECTORY + File.separatorChar + cassandraExecutable();
	}

	private String cassandraExecutable() {
		OperatingSystem operatingSystem = this.operatingSystemResolver.currentOperatingSystem();

		switch (operatingSystem.getFamily()) {
		case WINDOWS:
			return CASSANDRA_EXECUTABLE_W;
		default:
			return CASSANDRA_EXECUTABLE_X;
		}

	}

	private File ensureTargetPathDoesNotExitsAndReturnCompositePath() {
		File dbPath = new File(targetPath);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
		return dbPath;
	}
	
	private void stopCassandra() {
		pwd.destroy();
	}

	private void addExtraCommandLineArgument(String argumentName, String argumentValue) {
		this.extraCommandArguments.put(argumentName, argumentValue);
	}

	private void addSingleCommandLineArgument(String argument) {
		this.singleCommandArguments.add(argument);
	}

	private void setPort(int port) {
		this.port = port;
	}

	private void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	
	private void setCassandraPath(String cassandraPath) {
		this.cassandraPath = cassandraPath;
	}
	
	private String getCassandraPath() {
		return cassandraPath;
	}
	
	protected void setOperatingSystemResolver(OperatingSystemResolver operatingSystemResolver) {
		this.operatingSystemResolver = operatingSystemResolver;
	}
	
	protected void setCommandLineExecutor(CommandLineExecutor commandLineExecutor) {
		this.commandLineExecutor = commandLineExecutor;
	}
	
}
