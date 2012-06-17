package com.lordofthejars.nosqlunit.mongodb;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.rules.ExternalResource;

import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.ConnectionManagement;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.core.OsNameSystemPropertyOperatingSystemResolver;
import com.mongodb.DBPort;

/**
 * Run a mongodb server before each test suite.
 */
public class ManagedMongoDb extends ExternalResource {

	private static final String LOCALHOST = "127.0.0.1";

	private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;

	protected static final String LOGPATH_ARGUMENT_NAME = "--logpath";
	protected static final String FORK_ARGUMENT_NAME = "--fork";
	protected static final String DBPATH_ARGUMENT_NAME = "--dbpath";
	protected static final String PORT_ARGUMENT_NAME= "--port";
	protected static final String DEFAULT_MONGO_LOGPATH = "logpath";
	protected static final String DEFAULT_MONGO_DBPATH = "mongo-dbpath";
	protected static final String DEFAULT_MONGO_TARGET_PATH = "target"
			+ File.separatorChar + "mongo-temp";

	protected static final String MONGODB_BINARY_DIRECTORY = "bin";

	protected static final String MONGODB_EXECUTABLE_X = "mongod";
	protected static final String MONGODB_EXECUTABLE_W = "mongod.exe";

	private String mongodPath = System.getProperty("MONGO_HOME");
	private int port = DBPort.PORT;
	
	private String targetPath = DEFAULT_MONGO_TARGET_PATH;
	private String dbRelativePath = DEFAULT_MONGO_DBPATH;
	private String logRelativePath = DEFAULT_MONGO_LOGPATH;

	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();
	private OperatingSystemResolver operatingSystemResolver = new OsNameSystemPropertyOperatingSystemResolver();
	private MongoDbLowLevelOps mongoDbLowLevelOps = new MongoDbLowLevelOps();

	private ManagedMongoDb() {
		super();
	}

	/**
	 * Builder to start mongodb server accordingly to your setup
	 */
	public static class MongoServerRuleBuilder {

		private ManagedMongoDb managedMongoDb;

		private MongoServerRuleBuilder() {
			this.managedMongoDb = new ManagedMongoDb();
		}

		public static MongoServerRuleBuilder newManagedMongoDbRule() {
			return new MongoServerRuleBuilder();
		}

		public MongoServerRuleBuilder mongodPath(String mongodPath) {
			this.managedMongoDb.setMongodPath(mongodPath);
			return this;
		}

		public MongoServerRuleBuilder port(int port) {
			this.managedMongoDb.setPort(port);
			return this;
		}

		public MongoServerRuleBuilder targetPath(String targetPath) {
			this.managedMongoDb.setTargetPath(targetPath);
			return this;
		}

		public MongoServerRuleBuilder dbRelativePath(String dbRelativePath) {
			this.managedMongoDb.setDbRelativePath(dbRelativePath);
			return this;
		}

		public MongoServerRuleBuilder logRelativePath(String logRelativePath) {
			this.managedMongoDb.setLogRelativePath(logRelativePath);
			return this;
		}


		public MongoServerRuleBuilder appendCommandLineArguments(
				String argumentName, String argumentValue) {
			this.managedMongoDb.addExtraCommandLineArgument(argumentName,
					argumentValue);
			return this;
		}

		public MongoServerRuleBuilder appendSingleCommandLineArguments(
				String argument) {
			this.managedMongoDb.addSingleCommandLineArgument(argument);
			return this;
		}

		
		public ManagedMongoDb build() {
			if (this.managedMongoDb.getMongodPath() == null) {
				throw new IllegalArgumentException(
						"No Path to MongoDb is provided.");
			}
			return this.managedMongoDb;
		}
	}

	@Override
	protected void before() throws Throwable {

		if (isServerNotStartedYet()) {

			File dbPath = ensureDbPathDoesNotExitsAndReturnCompositePath();

			if (dbPath.mkdirs()) {
				startMongoDBAsADaemon();
				boolean isServerUp = assertThatConnectionToMongoDbIsPossible(NUM_RETRIES_TO_CHECK_SERVER_UP);

				if (!isServerUp) {
					throw new IllegalStateException(
							"Couldn't establish a connection with "
									+ this.mongodPath
									+ " server at /127.0.0.1:"+port);
				}

			} else {
				throw new IllegalStateException("Db Path " + dbPath
						+ " could not be created.");
			}
		}
		ConnectionManagement.getInstance()
				.addConnection(LOCALHOST, port);
	}

	private boolean isServerNotStartedYet() {
		return !ConnectionManagement.getInstance().isConnectionRegistered(
				LOCALHOST, port);
	}

	@Override
	protected void after() {
		int remainingConnections = ConnectionManagement.getInstance()
				.removeConnection(LOCALHOST, port);
		if (noMoreConnectionsToManage(remainingConnections)) {
			try {
				this.mongoDbLowLevelOps.shutdown(LOCALHOST, port);
			} finally {
				ensureDbPathDoesNotExitsAndReturnCompositePath();
			}
		}
	}

	private boolean noMoreConnectionsToManage(int remainingConnections) {
		return remainingConnections < 1;
	}

	private List<String> startMongoDBAsADaemon() throws InterruptedException {

		Process pwd;
		try {
			pwd = startProcess();
			List<String> lines = getConsoleOutput(pwd);
			pwd.waitFor();
			if (pwd.exitValue() != 0) {
				throw new IllegalStateException(
						"Mongodb ["
								+ mongodPath
								+ DBPATH_ARGUMENT_NAME
								+ dbRelativePath
								+ PORT_ARGUMENT_NAME
								+ port
								+ FORK_ARGUMENT_NAME
								+ LOGPATH_ARGUMENT_NAME
								+ logRelativePath
								+ "] could not be started. Next console message was thrown: "
								+ lines);
			}
			return lines;
		} catch (IOException e) {
			throw new IllegalStateException(
					"Mongodb ["
							+ mongodPath
							+ DBPATH_ARGUMENT_NAME
							+ dbRelativePath
							+ PORT_ARGUMENT_NAME
							+ port
							+ FORK_ARGUMENT_NAME
							+ LOGPATH_ARGUMENT_NAME
							+ logRelativePath
							+ "] could not be started. Next console message was thrown: "
							+ e.getMessage());
		}
	}

	private Process startProcess() throws IOException {
		return this.commandLineExecutor.startProcessInDirectoryAndArguments(
				targetPath, buildOperationSystemProgramAndArguments());
	}

	private List<String> getConsoleOutput(Process pwd) throws IOException {
		return this.commandLineExecutor.getConsoleOutput(pwd);
	}

	private List<String> buildOperationSystemProgramAndArguments() {

		List<String> programAndArguments = new ArrayList<String>();

		programAndArguments.add(getExecutablePath());
		programAndArguments.add(DBPATH_ARGUMENT_NAME);
		programAndArguments.add(dbRelativePath);
		programAndArguments.add(PORT_ARGUMENT_NAME);
		programAndArguments.add(Integer.toString(port));
		programAndArguments.add(FORK_ARGUMENT_NAME);
		programAndArguments.add(LOGPATH_ARGUMENT_NAME);
		programAndArguments.add(logRelativePath);

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
		return this.mongodPath + File.separatorChar + MONGODB_BINARY_DIRECTORY
				+ File.separatorChar + mongoExecutable();
	}

	private String mongoExecutable() {
		OperatingSystem operatingSystem = this.operatingSystemResolver
				.currentOperatingSystem();

		switch (operatingSystem.getFamily()) {
		case WINDOWS:
			return MONGODB_EXECUTABLE_W;
		default:
			return MONGODB_EXECUTABLE_X;
		}

	}

	private boolean assertThatConnectionToMongoDbIsPossible(int retries)
			throws InterruptedException, UnknownHostException {
		return this.mongoDbLowLevelOps.assertThatConnectionIsPossible(LOCALHOST, port, retries);
	}

	private File ensureDbPathDoesNotExitsAndReturnCompositePath() {
		File dbPath = new File(targetPath + File.separatorChar + dbRelativePath);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
		return dbPath;
	}

	private void setDbRelativePath(String dbRelativePath) {
		this.dbRelativePath = dbRelativePath;
	}

	private void setLogRelativePath(String logRelativePath) {
		this.logRelativePath = logRelativePath;
	}

	private void setMongodPath(String mongodPath) {
		this.mongodPath = mongodPath;
	}

	private void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

	private void addExtraCommandLineArgument(String argumentName,
			String argumentValue) {
		this.extraCommandArguments.put(argumentName, argumentValue);
	}

	private void addSingleCommandLineArgument(String argument) {
		this.singleCommandArguments.add(argument);
	}

	private void setPort(int port) {
		this.port = port;
	}
	
	private String getMongodPath() {
		return mongodPath;
	}

	protected void setCommandLineExecutor(
			CommandLineExecutor commandLineExecutor) {
		this.commandLineExecutor = commandLineExecutor;
	}

	protected void setOperatingSystemResolver(
			OperatingSystemResolver operatingSystemResolver) {
		this.operatingSystemResolver = operatingSystemResolver;
	}

	protected void setMongoDbLowLevelOps(MongoDbLowLevelOps mongoDbLowLevelOps) {
		this.mongoDbLowLevelOps = mongoDbLowLevelOps;
	}
}
