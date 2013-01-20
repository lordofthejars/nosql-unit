package com.lordofthejars.nosqlunit.couchdb;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.core.OsNameSystemPropertyOperatingSystemResolver;
import com.lordofthejars.nosqlunit.env.SystemEnvironmentVariables;

public class ManagedCouchDbLifecycleManager extends AbstractLifecycleManager {

	private static final String STARTED_CORRECTLY_MESSAGE = "Time to relax";

	private static final int TIME_TO_REALAX_INDEX = 1;

	Process pwd;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ManagedCouchDbLifecycleManager.class);

	protected static final String LOCALHOST = "localhost";
	protected static final int DEFAULT_PORT = 5984;

	protected static final String DEFAULT_COUCHDB_TARGET_PATH = "target" + File.separatorChar + "couchdb-temp";

	protected static final String COUCHDB_BINARY_DIRECTORY = "bin";
	protected static final String START_COUCHDB_EXECUTABLE_X = "couchdb";
	protected static final String START_COUCHDB_EXECUTABLE_W = "couchdb.bat";

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();
	private OperatingSystemResolver operatingSystemResolver = new OsNameSystemPropertyOperatingSystemResolver();

	private String targetPath = DEFAULT_COUCHDB_TARGET_PATH;

	private String couchDbPath = SystemEnvironmentVariables.getEnvironmentOrPropertyVariable("COUCHDB_HOME");

	private int port = DEFAULT_PORT;

	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();

	@Override
	public String getHost() {
		return LOCALHOST;
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public void doStart() throws Throwable {

		LOGGER.info("Starting {} CouchDb instance.", couchDbPath);

		File targetPathDirectory = ensureTargetPathDoesNotExitsAndReturnCompositePath();

		if (targetPathDirectory.mkdirs()) {
			startCouchDb();
		} else {
			throw new IllegalStateException("Target Path " + targetPathDirectory + " could not be created.");
		}

		LOGGER.info("Started {} CouchDb instance.", couchDbPath);

	}

	private void startCouchDb() {
		final CountDownLatch startupLatch = new CountDownLatch(1);
		new Thread(new Runnable() {
			public void run() {
				try {
					startCouchDbAsDaemon();
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
	
	private List<String> startCouchDbAsDaemon() throws InterruptedException {
		try {
			pwd = startProcess();
			List<String> lines = getConsoleOutput(pwd);
			pwd.waitFor();
			if (pwd.exitValue() != 0 && hasNotStartedCorrectly(lines)) {
				throw new IllegalStateException("CouchDb start [" + couchDbPath + " port " + port
						+ "] could not be started. Next console message was thrown: " + lines);
			}
			return lines;
		} catch (IOException e) {
			throw new IllegalStateException("CouchDb start [" + couchDbPath + " port " + port
					+ "] could not be started. Next console message was thrown: " + e.getMessage());
		}
	}

	private boolean hasNotStartedCorrectly(List<String> consoleOutput) {
		return consoleOutput.get(TIME_TO_REALAX_INDEX).indexOf(STARTED_CORRECTLY_MESSAGE) < 0;
	}
	
	private List<String> getConsoleOutput(Process pwd) throws IOException {
		return this.commandLineExecutor.getConsoleOutput(pwd);
	}

	private Process startProcess() throws IOException {
		return this.commandLineExecutor.startProcessInDirectoryAndArguments(targetPath,
				buildOperationSystemProgramAndArguments());
	}

	private List<String> buildOperationSystemProgramAndArguments() {

		List<String> programAndArguments = new ArrayList<String>();

		programAndArguments.add(getExecutablePath());

		for (String argument : this.singleCommandArguments) {
			programAndArguments.add(argument);
		}

		for (String argumentName : this.extraCommandArguments.keySet()) {
			programAndArguments.add(argumentName);
			programAndArguments.add(this.extraCommandArguments.get(argumentName));
		}

		return programAndArguments;

	}

	private String getExecutablePath() {
		return this.couchDbPath + File.separatorChar + COUCHDB_BINARY_DIRECTORY + File.separatorChar
				+ couchDbExecutable();
	}

	private String couchDbExecutable() {
		OperatingSystem operatingSystem = this.operatingSystemResolver.currentOperatingSystem();

		switch (operatingSystem.getFamily()) {
		case WINDOWS:
			return START_COUCHDB_EXECUTABLE_W;
		default:
			return START_COUCHDB_EXECUTABLE_X;
		}

	}

	private File ensureTargetPathDoesNotExitsAndReturnCompositePath() {
		File dbPath = new File(targetPath);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
		return dbPath;
	}

	@Override
	public void doStop() {
		LOGGER.info("Stopping {} HBase instance.", couchDbPath);

		try {
			stopCouchDb();
		} catch (InterruptedException e) {
			throw new IllegalArgumentException(e);
		} finally {
			ensureTargetPathDoesNotExitsAndReturnCompositePath();
		}

		LOGGER.info("Stopped {} HBase instance.", couchDbPath);
	}
	
	private void stopCouchDb() throws InterruptedException {
		if (isProcessAlive()) {
			pwd.destroy();
			TimeUnit.SECONDS.sleep(2);
		}
	}

	private boolean isProcessAlive() {
		return pwd != null;
	}
	
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setCouchDbPath(String couchDbPath) {
		this.couchDbPath = couchDbPath;
	}

	public void addExtraCommandLineArgument(String argumentName, String argumentValue) {
		this.extraCommandArguments.put(argumentName, argumentValue);
	}

	public void addSingleCommandLineArgument(String argument) {
		this.singleCommandArguments.add(argument);
	}

	public String getCouchDbPath() {
		return couchDbPath;
	}

	public void setOperatingSystemResolver(OperatingSystemResolver operatingSystemResolver) {
		this.operatingSystemResolver = operatingSystemResolver;
	}

	public void setCommandLineExecutor(CommandLineExecutor commandLineExecutor) {
		this.commandLineExecutor = commandLineExecutor;
	}

}
