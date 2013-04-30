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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.env.SystemEnvironmentVariables;

public class ManagedCassandraLifecycleManager extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(ManagedCassandra.class);

	private static final String MAXIUM_HEAP = "-Xmx1G";

	private static final String MINIUM_HEAP = "-Xms1G";

	private static final String ENABLE_ASSERTIONS = "-ea";

	private static final String LIB_DIRECTORY = "/lib";

	Process pwd;

	private static final String LOCALHOST = "127.0.0.1";

	protected static final String FOREGROUND_ARGUMENT_NAME = "-Dcassandra-foreground=yes";

	public static final String DEFAULT_CASSANDRA_TARGET_PATH = "target" + File.separatorChar + "cassandra-temp";
	protected static final String CASSANDRA_BINARY_DIRECTORY = "bin";

	protected String CASSANDRA_CONF_DIRECTORY = "/conf";
    protected String CASSANDRA_DAEMON_CLASS = "org.apache.cassandra.service.CassandraDaemon";

	protected String javaHome = System.getProperty("java.home");
	private String cassandraPath = SystemEnvironmentVariables.getEnvironmentOrPropertyVariable("CASSANDRA_HOME");

	protected static final String CASSANDRA_EXECUTABLE_X = "cassandra";
	protected static final String CASSANDRA_EXECUTABLE_W = "cassandra.bat";

	private String targetPath = DEFAULT_CASSANDRA_TARGET_PATH;
	private int port = CassandraHost.DEFAULT_PORT;

	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();

	public ManagedCassandraLifecycleManager() {
		super();
	}

	
	@Override
	public void doStart() throws Throwable {
		LOGGER.info("Starting {} Cassandra instance.", cassandraPath);
		startCassandra();
		LOGGER.info("Started {} Cassandra instance.", cassandraPath);
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
		LOGGER.info("Stopping {} Cassandra instance.", cassandraPath);
		
		stopCassandra();
		
		LOGGER.info("Stopped {} Cassandra instance.", cassandraPath);
	}

	private void stopCassandra() {
        if (pwd != null)
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

	public void addExtraCommandLineArgument(String argumentName, String argumentValue) {
		this.extraCommandArguments.put(argumentName, argumentValue);
	}

	public void addSingleCommandLineArgument(String argument) {
		this.singleCommandArguments.add(argument);
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

	public void setCassandraPath(String cassandraPath) {
		this.cassandraPath = cassandraPath;
	}

	public String getCassandraPath() {
		return cassandraPath;
	}

	public void setCommandLineExecutor(CommandLineExecutor commandLineExecutor) {
		this.commandLineExecutor = commandLineExecutor;
	}

	@Override
	public String getHost() {
		return LOCALHOST;
	}

	@Override
	public int getPort() {
		return this.port;
	}

}
