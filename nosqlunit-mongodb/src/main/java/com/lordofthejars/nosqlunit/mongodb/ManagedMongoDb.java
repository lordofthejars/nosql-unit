package com.lordofthejars.nosqlunit.mongodb;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.core.OsNameSystemPropertyOperatingSystemResolver;
import com.mongodb.DBPort;

/**
 * Run a mongodb server before each test suite.
 */
public class ManagedMongoDb extends AbstractLifecycleManager {

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

	
	
	private static final String LOCALHOST = "127.0.0.1";

	private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;

	protected static final String LOGPATH_ARGUMENT_NAME = "--logpath";
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

	@Override
	protected String getHost() {
		return LOCALHOST;
	}

	@Override
	protected int getPort() {
		return this.port;
	}

	@Override
	protected void doStart() throws Throwable {
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

	@Override
	protected void doStop() {
		try {
			this.mongoDbLowLevelOps.shutdown(LOCALHOST, port);
		} finally {
			ensureDbPathDoesNotExitsAndReturnCompositePath();
		}
	}
	

	private List<String> startMongoDBAsADaemon() throws InterruptedException {
        CountDownLatch processIsReady = new CountDownLatch(1);
        ProcessRunnable processRunnable = new ProcessRunnable(processIsReady);
        Thread thread = new Thread(processRunnable);
        thread.start();
        processIsReady.await();
        return processRunnable.consoleOutput;
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

	public void setDbRelativePath(String dbRelativePath) {
		this.dbRelativePath = dbRelativePath;
	}

	public void setLogRelativePath(String logRelativePath) {
		this.logRelativePath = logRelativePath;
	}

	public void setMongodPath(String mongodPath) {
		this.mongodPath = mongodPath;
	}

	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

	public void addExtraCommandLineArgument(String argumentName,
			String argumentValue) {
		this.extraCommandArguments.put(argumentName, argumentValue);
	}

	public void addSingleCommandLineArgument(String argument) {
		this.singleCommandArguments.add(argument);
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	protected String getMongodPath() {
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


    public class ProcessRunnable implements Runnable {

        private CountDownLatch processIsReady;
        private List<String> consoleOutput;

        public ProcessRunnable(CountDownLatch processIsReady) {
            this.processIsReady = processIsReady;
        }

        @Override
        public void run() {
            Process pwd;
            try {
                pwd = startProcess();
                consoleOutput = getConsoleOutput(pwd);
            } catch (IOException e) {
                throw prepareException(e);
            } finally {
                processIsReady.countDown();
            }

            try {
                pwd.waitFor();
                if (pwd.exitValue() != 0) {
                    throw new IllegalStateException(
                            "Mongodb ["
                                    + mongodPath
                                    + DBPATH_ARGUMENT_NAME
                                    + dbRelativePath
                                    + PORT_ARGUMENT_NAME
                                    + port
                                    + LOGPATH_ARGUMENT_NAME
                                    + logRelativePath
                                    + "] could not be started. Next console message was thrown: "
                                    + consoleOutput);
                }
            } catch (InterruptedException ie) {
                throw prepareException(ie);
            }

        }

        private IllegalStateException prepareException(Exception e) {
            return new IllegalStateException(
                    "Mongodb ["
                            + mongodPath
                            + DBPATH_ARGUMENT_NAME
                            + dbRelativePath
                            + PORT_ARGUMENT_NAME
                            + port
                            + LOGPATH_ARGUMENT_NAME
                            + logRelativePath
                            + "] could not be started. Next console message was thrown: "
                            + e.getMessage());
        }
    }

}
