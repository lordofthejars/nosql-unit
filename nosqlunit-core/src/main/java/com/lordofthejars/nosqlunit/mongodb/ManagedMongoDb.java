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
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.core.OsNameSystemPropertyOperatingSystemResolver;

/**
 * Run a mongodb server before each test suite.
 */
public class ManagedMongoDb extends ExternalResource {

	protected static final String LOGPATH_ARGUMENT_NAME = "--logpath";
	protected static final String FORK_ARGUMENT_NAME = "--fork";
	protected static final String DBPATH_ARGUMENT_NAME = "--dbpath";
	protected static final String DEFAULT_MONGO_LOGPATH = "logpath";
	protected static final String DEFAULT_MONGO_DBPATH = "mongo-dbpath";
	protected static final String DEFAULT_MONGO_TARGET_PATH = "target"
			+ File.separatorChar + "mongo-temp";

	protected static final String MONGODB_BINARY_DIRECTORY = "bin";
	
	protected static final String MONGODB_EXECUTABLE_X = "mongod";
	protected static final String MONGODB_EXECUTABLE_W = "mongod.exe";

	private String mongodPath = System.getProperty("MONGO_HOME");

	private String targetPath = DEFAULT_MONGO_TARGET_PATH;
	private String dbRelativePath = DEFAULT_MONGO_DBPATH;
	private String logRelativePath = DEFAULT_MONGO_LOGPATH;

	private Map<String, String> extraCommandArguments = new HashMap<String, String>();

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

		File dbPath = ensureDbPathDoesNotExitsAndReturnCompositePath();

		if (dbPath.mkdirs()) {
			startMongoDBAsADaemon();
			assertThatConnectionToMongoDbIsPossible();
		} else {
			throw new IllegalStateException("Db Path " + dbPath
					+ " could not be created.");
		}

	}

	@Override
	protected void after() {
		try {
			this.mongoDbLowLevelOps.shutdown();
		}finally {
			ensureDbPathDoesNotExitsAndReturnCompositePath();
		}
	}

	private List<String> startMongoDBAsADaemon() 
			throws InterruptedException {

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
							+ FORK_ARGUMENT_NAME
							+ LOGPATH_ARGUMENT_NAME
							+ logRelativePath
							+ "] could not be started. Next console message was thrown: "
							+ e.getMessage());
		}
	}

	private Process startProcess() throws IOException {
		return this.commandLineExecutor.startProcessInDirectoryAndArguments(targetPath, buildOperationSystemProgramAndArguments());
	}

	private List<String> getConsoleOutput(Process pwd) throws IOException {
		return this.commandLineExecutor.getConsoleOutput(pwd);
	}

	private List<String> buildOperationSystemProgramAndArguments() {

		List<String> programAndArguments = new ArrayList<String>();

		programAndArguments.add(getExecutablePath());
		programAndArguments.add(DBPATH_ARGUMENT_NAME);
		programAndArguments.add(dbRelativePath);
		programAndArguments.add(FORK_ARGUMENT_NAME);
		programAndArguments.add(LOGPATH_ARGUMENT_NAME);
		programAndArguments.add(logRelativePath);

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
		OperatingSystem operatingSystem = this.operatingSystemResolver.currentOperatingSystem();
		
		switch(operatingSystem.getFamily()) {
			case WINDOWS: return MONGODB_EXECUTABLE_W;
			default: return MONGODB_EXECUTABLE_X;
		}
		
	}
	
	private void assertThatConnectionToMongoDbIsPossible()
			throws InterruptedException, UnknownHostException {
		this.mongoDbLowLevelOps.assertThatConnectionIsPossible();
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

	private String getMongodPath() {
		return mongodPath;
	}

	protected void setCommandLineExecutor(CommandLineExecutor commandLineExecutor) {
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
