package com.lordofthejars.nosqlunit.cassandra;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectUnique;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.StringStartsWith.startsWith;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import me.prettyprint.cassandra.service.CassandraHost;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.CommandLineExecutor;

public class ManagedCassandra extends AbstractLifecycleManager {

	private static final String MAXIUM_HEAP = "-Xmx1G";

	private static final String MINIUM_HEAP = "-Xms1G";

	private static final String ENABLE_ASSERTIONS = "-ea";

	private static final String LIB_DIRECTORY = "/lib";

	Process pwd;

	private static final String LOCALHOST = "127.0.0.1";

	protected static final String FOREGROUND_ARGUMENT_NAME = "-Dcassandra-foreground=yes";

	protected static final String DEFAULT_CASSANDRA_TARGET_PATH = "target" + File.separatorChar + "cassandra-temp";
	protected static final String CASSANDRA_BINARY_DIRECTORY = "bin";

	protected String CASSANDRA_CONF_DIRECTORY = "/conf";
	protected String CASSANDRA_DAEMON_CLASS = "org.apache.cassandra.thrift.CassandraDaemon";

	protected String javaHome = System.getProperty("java.home");
	private String cassandraPath = System.getProperty("CASSANDRA_HOME");

	protected static final String CASSANDRA_EXECUTABLE_X = "cassandra";
	protected static final String CASSANDRA_EXECUTABLE_W = "cassandra.bat";

	private String targetPath = DEFAULT_CASSANDRA_TARGET_PATH;
	private int port = CassandraHost.DEFAULT_PORT;

	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();

	public ManagedCassandra() {
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
			if (this.managedCassandra.getCassandraPath() == null) {
				throw new IllegalArgumentException("Cassandra Path cannot be null.");
			}

			return this.managedCassandra;
		}

	}

	@Override
	public void doStart() throws Throwable {
		startCassandra();
	}

	private void startCassandra() throws AssertionError {
		final CountDownLatch startupLatch = new CountDownLatch(1);
		new Thread(new Runnable() {
			public void run() {
				try {
					startCassandraAsDaemon();
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

	private List<String> startCassandraAsDaemon() throws InterruptedException {

		try {
			pwd = startProcess();
			pwd.waitFor();
			if (pwd.exitValue() != 0) {
				List<String> consoleOutput = getConsoleOutput(pwd);
				throw new IllegalStateException("Cassandra [" + cassandraPath + " at port " + port
						+ "] could not be started. Next console message was thrown: " + consoleOutput);
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

		File[] cassandraJarLibraries = getCassandraJarLibraries();

		List<String> programAndArguments = new ArrayList<String>();
		File jammJar = getJammJar(cassandraJarLibraries);

		String classpath = getCassandraClasspath(cassandraJarLibraries);

		programAndArguments.add(javaHome + "/bin/java");
		programAndArguments.add(ENABLE_ASSERTIONS);
		programAndArguments.add("-javaagent:\"" + jammJar.getAbsolutePath() + "\"");
		programAndArguments.add(MINIUM_HEAP);
		programAndArguments.add(MAXIUM_HEAP);
		programAndArguments.add(FOREGROUND_ARGUMENT_NAME);

		for (String argument : this.singleCommandArguments) {
			programAndArguments.add(argument);
		}

		for (String argumentName : this.extraCommandArguments.keySet()) {
			programAndArguments.add(argumentName);
			programAndArguments.add(this.extraCommandArguments.get(argumentName));
		}

		programAndArguments.add("-cp");
		programAndArguments.add("\"" + cassandraPath + CASSANDRA_CONF_DIRECTORY + "\";" + classpath + "");
		programAndArguments.add(CASSANDRA_DAEMON_CLASS);

		return programAndArguments;

	}

	@Override
	public void doStop() {
			stopCassandra();
	}

	private void stopCassandra() {
		pwd.destroy();
	}


	private File[] getCassandraJarLibraries() {
		File cassandraLibDirectory = new File(cassandraPath + LIB_DIRECTORY);
		File[] cassandraJars = cassandraLibDirectory.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith(".jar");
			}
		});

		return cassandraJars;
	}

	private File getJammJar(File[] cassandraJars) {
		File jammJar = selectUnique(cassandraJars, having(on(File.class).getName(), startsWith("jamm")));
		return jammJar;
	}

	private String getCassandraClasspath(File[] cassandraJars) {

		StringBuilder classpathCommandLine = new StringBuilder();

		for (File cassandraJar : cassandraJars) {
			classpathCommandLine.append("\"").append(cassandraJar.getAbsolutePath()).append("\";");
		}

		return classpathCommandLine.substring(0, classpathCommandLine.length() - 1);

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

	protected void setCommandLineExecutor(CommandLineExecutor commandLineExecutor) {
		this.commandLineExecutor = commandLineExecutor;
	}

	@Override
	protected String getHost() {
		return LOCALHOST;
	}

	@Override
	protected int getPort() {
		return this.port;
	}

}
