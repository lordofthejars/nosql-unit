package com.lordofthejars.nosqlunit.infinispan;

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

public class ManagedInfinispanLifecycleManager extends AbstractLifecycleManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(ManagedInfinispanLifecycleManager.class);
	
	Process pwd;
	
	private static final String LOCALHOST = "127.0.0.1";
	public static final int DEFAULT_PORT = 11222;
	
	protected static final String DEFAULT_INFINISPAN_TARGET_PATH = "target" + File.separatorChar + "infinispan-temp";
	protected static final String INFINISPAN_BINARY_DIRECTORY = "bin";

	protected static final String INFINISPAN_EXECUTABLE_X = "standalone.sh";
	protected static final String INFINISPAN_EXECUTABLE_W = "standalone.bat";

	protected static final String PORT_COMMAND_LINE_ARGUMENT_NAME = "--port";
	protected static final String PROTOCOL_COMMAND_LINE_ARGUMENT_NAME = "--protocol";
	protected static final String PROTOCOL_COMMAND_LINE_DEFAULT_VALUE = "hotrod";
	
	private int port = DEFAULT_PORT;
	private String protocol = PROTOCOL_COMMAND_LINE_DEFAULT_VALUE;
	
	private String targetPath = DEFAULT_INFINISPAN_TARGET_PATH;
	private String infinispanPath = SystemEnvironmentVariables.getEnvironmentOrPropertyVariable("INFINISPAN_HOME");
	
	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();
	private OperatingSystemResolver operatingSystemResolver = new OsNameSystemPropertyOperatingSystemResolver();
	
	private boolean closing = false;
	
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
		
		LOGGER.info("Starting {} Infinispan instance.", infinispanPath);
		

		File targetPathDirectory = ensureTargetPathDoesNotExitsAndReturnCompositePath();

		if (targetPathDirectory.mkdirs()) {
			startInfinispanAsDaemon();
		} else {
			throw new IllegalStateException("Target Path " + targetPathDirectory + " could not be created.");
		}
		
		LOGGER.info("Started {} Infinispan instance.", infinispanPath);
	}

	private void startInfinispanAsDaemon() {
		final CountDownLatch startupLatch = new CountDownLatch(1);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					startInfinispanProcess();
					startupLatch.countDown();
				} catch (InterruptedException e) {
					throw new IllegalStateException(e);
				}
			}

		}).start();

		try {
			startupLatch.await(5, SECONDS);
		} catch (InterruptedException e) {
			throw new AssertionError(e);
		}
	}

	private List<String> startInfinispanProcess() throws InterruptedException {
		try {
			pwd = startProcess();
			pwd.waitFor();
			if (pwd.exitValue() != 0) {
				List<String> consoleOutput = getConsoleOutput(pwd);
				throw new IllegalStateException("Infinispan [" + infinispanPath + " at port " + port + " and protocol "+protocol
						+ "] could not be started. Next console message was thrown: " + consoleOutput);
			}
			return null;
		} catch (IOException e) {
			if(!closing) {
				throw new IllegalStateException("Infinispan [" + infinispanPath + " at port " + port + " and protocol "+protocol
					+ "] could not be started. Next console message was thrown: " + e.getMessage());
			}
			return null;
		}
	}
	
	private Process startProcess() throws IOException {
		return this.commandLineExecutor.startProcessInDirectoryAndArguments(targetPath,
				buildOperationSystemProgramAndArguments());
	}
	
	private List<String> buildOperationSystemProgramAndArguments() {

		List<String> programAndArguments = new ArrayList<String>();

		programAndArguments.add(getExecutablePath());
//		addPort(programAndArguments);
//		addProtocol(programAndArguments);
		
		addSingleArguments(programAndArguments);
		addCommandArguments(programAndArguments);

		return programAndArguments;

	}

	private void addCommandArguments(List<String> programAndArguments) {
		for (String argumentName : this.extraCommandArguments.keySet()) {
			programAndArguments.add(argumentName);
			programAndArguments.add(this.extraCommandArguments.get(argumentName));
		}
	}

	private void addSingleArguments(List<String> programAndArguments) {
		for (String argument : this.singleCommandArguments) {
			programAndArguments.add(argument);
		}
	}
	
	
	private void addProtocol(List<String> programAndArguments) {
		programAndArguments.add(PROTOCOL_COMMAND_LINE_ARGUMENT_NAME);
		programAndArguments.add(PROTOCOL_COMMAND_LINE_DEFAULT_VALUE);
	}

	private void addPort(List<String> programAndArguments) {
		programAndArguments.add(PORT_COMMAND_LINE_ARGUMENT_NAME);
		programAndArguments.add(Integer.toString(port));
	}

	
	
	private String getExecutablePath() {
		return this.infinispanPath + File.separatorChar + INFINISPAN_BINARY_DIRECTORY + File.separatorChar + infinispanExecutable();
	}

	private String infinispanExecutable() {
		OperatingSystem operatingSystem = this.operatingSystemResolver.currentOperatingSystem();

		switch (operatingSystem.getFamily()) {
		case WINDOWS:
			return INFINISPAN_EXECUTABLE_W;
		default:
			return INFINISPAN_EXECUTABLE_X;
		}

	}
	
	private List<String> getConsoleOutput(Process pwd) throws IOException {
		return this.commandLineExecutor.getConsoleOutput(pwd);
	}
	
	@Override
	public void doStop() {
		
		LOGGER.info("Stopping {} Infinispan instance.", infinispanPath);
		
		try {
			closing = true;
			stopInfinispan();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		} finally {
			ensureTargetPathDoesNotExitsAndReturnCompositePath();
		}
		
		LOGGER.info("Stopped {} Infinispan instance.", infinispanPath);
	}

	private void stopInfinispan() throws InterruptedException {
		if (isProcessAlive()) {
			pwd.destroy();
			TimeUnit.SECONDS.sleep(2);
		}
	}

	private boolean isProcessAlive() {
		return pwd != null;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	
	public String getProtocol() {
		return protocol;
	}
	
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
	
	public void setInfinispanPath(String infinispanPath) {
		this.infinispanPath = infinispanPath;
	}
	
	public void addExtraCommandLineArgument(String argumentName, String argumentValue) {
		this.extraCommandArguments.put(argumentName, argumentValue);
	}

	public void addSingleCommandLineArgument(String argument) {
		this.singleCommandArguments.add(argument);
	}
	
	public void setCommandLineExecutor(CommandLineExecutor commandLineExecutor) {
		this.commandLineExecutor = commandLineExecutor;
	}
	
	public void setOperatingSystemResolver(OperatingSystemResolver operatingSystemResolver) {
		this.operatingSystemResolver = operatingSystemResolver;
	}
	
	private File ensureTargetPathDoesNotExitsAndReturnCompositePath() {
		File dbPath = new File(targetPath);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
		return dbPath;
	}
	
}
