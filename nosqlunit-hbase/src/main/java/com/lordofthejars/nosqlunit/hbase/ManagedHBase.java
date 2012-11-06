package com.lordofthejars.nosqlunit.hbase;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.core.OsNameSystemPropertyOperatingSystemResolver;
import com.lordofthejars.nosqlunit.env.SystemEnvironmentVariables;

public class ManagedHBase extends AbstractLifecycleManager {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ManagedHBase.class); 
	
	private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;
	
	protected static final String LOCALHOST = "127.0.0.1";
	protected static final int PORT = HConstants.DEFAULT_MASTER_PORT;
	
	
	protected static final String DEFAULT_HBASE_TARGET_PATH = "target" + File.separatorChar + "hbase-temp";
		
	protected static final String HBASE_BINARY_DIRECTORY = "bin";
	protected static final String START_HBASE_EXECUTABLE_X = "start-hbase.sh";
	protected static final String STOP_HBASE_EXECUTABLE_X = "stop-hbase.sh";

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();
	private OperatingSystemResolver operatingSystemResolver = new OsNameSystemPropertyOperatingSystemResolver();
	private HBaseUtils hBaseUtils = new HBaseUtils();
	
	private String targetPath = DEFAULT_HBASE_TARGET_PATH;

	private String hBasePath = SystemEnvironmentVariables.getEnvironmentOrPropertyVariable("HBASE_HOME");

	private int port = HConstants.DEFAULT_MASTER_PORT;
	
	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();
	
	public static class HBaseRuleBuilder {

		private ManagedHBase managedHBaseServer;

		private HBaseRuleBuilder() {
			this.managedHBaseServer = new ManagedHBase();
		}

		public static HBaseRuleBuilder newManagedHBaseServerRule() {
			return new HBaseRuleBuilder();
		}

		public HBaseRuleBuilder hBasePath(String hBasePath) {
			this.managedHBaseServer.setHBasePath(hBasePath);
			return this;
		}

		public HBaseRuleBuilder targetPath(String targetPath) {
			this.managedHBaseServer.setTargetPath(targetPath);
			return this;
		}

		public HBaseRuleBuilder port(int port) {
			this.managedHBaseServer.setPort(port);
			return this;
		}

		public HBaseRuleBuilder appendCommandLineArguments(String argumentName, String argumentValue) {
			this.managedHBaseServer.addExtraCommandLineArgument(argumentName, argumentValue);
			return this;
		}

		public HBaseRuleBuilder appendSingleCommandLineArguments(String argument) {
			this.managedHBaseServer.addSingleCommandLineArgument(argument);
			return this;
		}
		
		public ManagedHBase build() {
			if (this.managedHBaseServer.getHBasePath() == null) {
				throw new IllegalArgumentException("No Path to HBase is provided.");
			}
			return this.managedHBaseServer;
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
		
		LOGGER.info("Starting {} HBase instance.", hBasePath);
		
		File targetPathDirectory = ensureTargetPathDoesNotExitsAndReturnCompositePath();

		if (targetPathDirectory.mkdirs()) {
			startHBaseAsADaemon();
			checkRunningServer();

		} else {	
			throw new IllegalStateException("Target Path " + targetPathDirectory
					+ " could not be created.");
		}
		
		LOGGER.info("Started {} HBase instance.", hBasePath);

	}

	private void checkRunningServer() throws InterruptedException {
		boolean isServerUp = assertThatConnectionIsPossible();

		if (!isServerUp) {
			throw new IllegalStateException("Couldn't establish a connection with " + this.hBasePath
					+ " server at /127.0.0.1:" + port);
		}
	}


	@Override
	protected void doStop() {
		
		LOGGER.info("Stopping {} HBase instance.", hBasePath);
		
		try {
			stopHBase();			
		} catch(InterruptedException e) {
			throw new IllegalArgumentException(e);
		} finally {
			ensureTargetPathDoesNotExitsAndReturnCompositePath();
		}
		
		LOGGER.info("Stopped {} HBase instance.", hBasePath);
	}
	
	private List<String> startHBaseAsADaemon() throws InterruptedException {
		Process pwd;
		try {
			pwd = startProcess();
			List<String> lines = getConsoleOutput(pwd);
			pwd.waitFor();
			if (pwd.exitValue() != 0) {
				throw new IllegalStateException("HBase start [" + hBasePath + "port " + port
						+ "] could not be started. Next console message was thrown: " + lines);
			}
			return lines;
		} catch (IOException e) {
			throw new IllegalStateException("HBase start [" + hBasePath + "port " + port
					+ "] could not be started. Next console message was thrown: " + e.getMessage());
		}
	}
	
	
	private boolean assertThatConnectionIsPossible() throws InterruptedException {

		Configuration config = HBaseConfiguration.create();

		for (int i = 0; i < NUM_RETRIES_TO_CHECK_SERVER_UP; i++) {
	         
			if(hBaseUtils.isConnectionPossible(config)) {
				return true;
			}
			

			TimeUnit.SECONDS.sleep(3);
		}

		return false;
	}
	
	private List<String> stopHBase() throws InterruptedException {

		Process pwd;
		try {
			pwd = stopProcess();
			List<String> lines = getConsoleOutput(pwd);
			pwd.waitFor();
			if (pwd.exitValue() != 0) {
				throw new IllegalStateException("HBase stop [" + hBasePath + "port " + port
						+ "] could not be started. Next console message was thrown: " + lines);
			}
			return lines;
		} catch (IOException e) {
			throw new IllegalStateException("HBase stop [" + hBasePath + "port " + port
					+ "] could not be started. Next console message was thrown: " + e.getMessage());
		}
	}
	
	
	private Process startProcess() throws IOException {
		return this.commandLineExecutor.startProcessInDirectoryAndArguments(targetPath,
				buildOperationSystemProgramAndArguments());
	}

	private Process stopProcess() throws IOException {
		return this.commandLineExecutor.startProcessInDirectoryAndArguments(targetPath,
				buildStopProgram());
	}

	private List<String> buildStopProgram() {
		List<String> programAndArguments = new ArrayList<String>();
		programAndArguments.add(getStoppingExecutablePath());
		return programAndArguments;
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

	private String getStoppingExecutablePath() {
		return this.hBasePath + File.separatorChar + HBASE_BINARY_DIRECTORY + File.separatorChar + STOP_HBASE_EXECUTABLE_X;
	}
	
	private String getExecutablePath() {
		return this.hBasePath + File.separatorChar + HBASE_BINARY_DIRECTORY + File.separatorChar +hBaseExecutable();
	}

	private String hBaseExecutable() {
		OperatingSystem operatingSystem = this.operatingSystemResolver.currentOperatingSystem();

		switch (operatingSystem.getFamily()) {
		case WINDOWS:
			throw new IllegalArgumentException("HBase is not supported in Windows Systems.");
		default:
			return START_HBASE_EXECUTABLE_X;
		}

	}

	private List<String> getConsoleOutput(Process pwd) throws IOException {
		return this.commandLineExecutor.getConsoleOutput(pwd);
	}


	private File ensureTargetPathDoesNotExitsAndReturnCompositePath() {
		File dbPath = new File(targetPath);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
		return dbPath;
	}

	private void setPort(int port) {
		this.port = port;
	}
	
	private void addExtraCommandLineArgument(String argumentName, String argumentValue) {
		this.extraCommandArguments.put(argumentName, argumentValue);
	}

	private void addSingleCommandLineArgument(String argument) {
		this.singleCommandArguments.add(argument);
	}
	
	private void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	
	private void setHBasePath(String hBasePath) {
		this.hBasePath = hBasePath;
	}
	
	private String getHBasePath() {
		return hBasePath;
	}
	
	
	protected void setOperatingSystemResolver(OperatingSystemResolver operatingSystemResolver) {
		this.operatingSystemResolver = operatingSystemResolver;
	}
	
	protected void setCommandLineExecutor(CommandLineExecutor commandLineExecutor) {
		this.commandLineExecutor = commandLineExecutor;
	}
	
	protected void setHBaseUtils(HBaseUtils hBaseUtils) {
		this.hBaseUtils = hBaseUtils;
	}
	
}
