package com.lordofthejars.nosqlunit.mongodb.shard;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;
import static com.lordofthejars.nosqlunit.util.CsvBuilder.joinFrom;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.core.OsNameSystemPropertyOperatingSystemResolver;
import com.lordofthejars.nosqlunit.env.SystemEnvironmentVariables;
import com.lordofthejars.nosqlunit.mongodb.MongoDbLowLevelOps;
import com.lordofthejars.nosqlunit.mongodb.MongoDbLowLevelOpsFactory;

public class ManagedMongosLifecycleManager extends AbstractLifecycleManager {

private static final Logger LOGGER = LoggerFactory.getLogger(ManagedMongosLifecycleManager.class); 
	
	public ManagedMongosLifecycleManager() {
		super();
	}
	
	private static final String LOCALHOST = "localhost";
	protected static final int DEFAULT_PORT = 27017;

	
	protected static final String LOGPATH_ARGUMENT_NAME = "--logpath";
	protected static final String PORT_ARGUMENT_NAME= "--port";
	protected static final String CHUNK_SIZE_ARGUMENT_NAME = "--chunkSize";
	protected static final String CONFIG_DB_ARGUMENT_NAME = "--configdb";

	protected static final String DEFAULT_MONGO_LOGPATH = "logpath";
	protected static final String DEFAULT_MONGO_TARGET_PATH = "target"
			+ File.separatorChar + "mongo-temp";
	protected static final int DEFAULT_CHUNK_SIZE = 1;
	
	protected static final String MONGODB_BINARY_DIRECTORY = "bin";

	protected static final String MONGOS_EXECUTABLE_X = "mongos";
	protected static final String MONGOS_EXECUTABLE_W = "mongos.exe";


	private static final String DEFAULT_PATH = "mongos";

	private String mongosPath = SystemEnvironmentVariables.getEnvironmentOrPropertyVariable("MONGO_HOME");
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	
	private int port = DEFAULT_PORT;
	
	private String targetPath = DEFAULT_MONGO_TARGET_PATH;
	private String logRelativePath = DEFAULT_MONGO_LOGPATH;

	private List<String> configDatabases = new ArrayList<String>();
	
	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();
	private OperatingSystemResolver operatingSystemResolver = new OsNameSystemPropertyOperatingSystemResolver();
	private MongoDbLowLevelOps mongoDbLowLevelOps = MongoDbLowLevelOpsFactory.getSingletonInstance();

	private ProcessRunnable processRunnable;
	
	@Override
	public String getHost() {
		return LOCALHOST;
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public void doStart() throws Throwable {
		
		LOGGER.info("Starting {} Mongos instance.", mongosPath);
		
		File dbPath = ensureDbPathDoesNotExitsAndReturnCompositePath();

		if (dbPath.mkdirs()) {
			startMongoDBAsADaemon();
			boolean isServerUp = assertThatConnectionToMongoDbIsPossible();

			if (!isServerUp) {
				throw new IllegalStateException(
						"Couldn't establish a connection with "
								+ this.mongosPath
								+ " server at /127.0.0.1:"+port);
			}

		} else {
			throw new IllegalStateException("Db Path " + dbPath
					+ " could not be created.");
		}
		
		LOGGER.info("Started {} Mongos instance.", mongosPath);
	}

	@Override
	public void doStop() {
		
		LOGGER.info("Stopping {} Mongos instance.", mongosPath);
		
		try {
			if(this.processRunnable != null) {
				this.processRunnable.destroyProcess();
			}
		} finally {
			ensureDbPathDoesNotExitsAndReturnCompositePath();
		}
		
		LOGGER.info("Stopped {} Mongos instance.", mongosPath);
	}


	private List<String> startMongoDBAsADaemon() throws InterruptedException {
        CountDownLatch processIsReady = new CountDownLatch(1);
        processRunnable = new ProcessRunnable(processIsReady);
        Thread thread = new Thread(processRunnable);
        thread.start();
        processIsReady.await();
        return processRunnable.consoleOutput;
	}


	private List<String> buildOperationSystemProgramAndArguments() {

		List<String> programAndArguments = new ArrayList<String>();

		programAndArguments.add(getExecutablePath());
		
		
		programAndArguments.add(PORT_ARGUMENT_NAME);
		programAndArguments.add(Integer.toString(port));
		programAndArguments.add(LOGPATH_ARGUMENT_NAME);
		programAndArguments.add(logRelativePath);
		programAndArguments.add(CHUNK_SIZE_ARGUMENT_NAME);
		programAndArguments.add(Integer.toString(chunkSize));
		programAndArguments.add(CONFIG_DB_ARGUMENT_NAME);
		programAndArguments.add(joinFrom(this.configDatabases).trim());
		
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
		return this.mongosPath + File.separatorChar + MONGODB_BINARY_DIRECTORY
				+ File.separatorChar + mongoExecutable();
	}

	private String mongoExecutable() {
		OperatingSystem operatingSystem = this.operatingSystemResolver
				.currentOperatingSystem();

		switch (operatingSystem.getFamily()) {
		case WINDOWS:
			return MONGOS_EXECUTABLE_W;
		default:
			return MONGOS_EXECUTABLE_X;
		}

	}

	private boolean assertThatConnectionToMongoDbIsPossible()
			throws InterruptedException, UnknownHostException {
		return this.mongoDbLowLevelOps.assertThatConnectionIsPossible(LOCALHOST, port);
	}

	private File ensureDbPathDoesNotExitsAndReturnCompositePath() {
		File dbPath = new File(targetPath + File.separatorChar + DEFAULT_PATH);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
		return dbPath;
	}


	public void setLogRelativePath(String logRelativePath) {
		this.logRelativePath = logRelativePath;
	}

	public void setMongosPath(String mongodPath) {
		this.mongosPath = mongodPath;
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
	
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}
	
	public void addConfigurationDatabase(String hostAndPort) {
		this.configDatabases.add(hostAndPort);
	}
	
	protected String getMongosPath() {
		return mongosPath;
	}

	protected boolean areConfigDatabasesDefined() {
		return configDatabases.size() > 0;
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

        private Process process;
        
        public ProcessRunnable(CountDownLatch processIsReady) {
            this.processIsReady = processIsReady;
        }

        @Override
        public void run() {
            try {
            	process = startProcess();
                //consoleOutput = getConsoleOutput(process);
            } catch (IOException e) {
                throw prepareException(e);
            } finally {
                processIsReady.countDown();
            }

            try {
            	process.waitFor();
                if (process.exitValue() != 0) {
                    LOGGER.info(
                            "Mongos ["
                                    + mongosPath
                                    + PORT_ARGUMENT_NAME
                                    + port
                                    + LOGPATH_ARGUMENT_NAME
                                    + logRelativePath
                                    + "] console output is: "
                                    + consoleOutput);
                }
            } catch (InterruptedException ie) {
                throw prepareException(ie);
            }

        }

        public void destroyProcess() {
        	if(this.process != null) {
        		this.process.destroy();
        	}
        }
        
        private IllegalStateException prepareException(Exception e) {
            return new IllegalStateException(
                    "Mongos ["
                            + mongosPath
                            + PORT_ARGUMENT_NAME
                            + port
                            + LOGPATH_ARGUMENT_NAME
                            + logRelativePath
                            + "] could not be started. Next console message was thrown: "
                            + e.getMessage());
        }
        
        private Process startProcess() throws IOException {
    		return commandLineExecutor.startProcessInDirectoryAndArguments(
    				targetPath, buildOperationSystemProgramAndArguments());
    	}
        
        private List<String> getConsoleOutput(Process pwd) throws IOException {
    		return commandLineExecutor.getConsoleOutput(pwd);
    	}
    }

}
