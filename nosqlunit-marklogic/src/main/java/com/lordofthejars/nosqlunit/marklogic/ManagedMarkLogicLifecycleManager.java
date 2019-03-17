package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;
import static com.lordofthejars.nosqlunit.env.SystemEnvironmentVariables.getEnvironmentVariable;
import static java.io.File.separatorChar;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class ManagedMarkLogicLifecycleManager extends AbstractLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedMarkLogicLifecycleManager.class);

    private static final String DEFAULT_MARKLOGIC_TARGET_PATH = "target" + separatorChar + "marklogic-temp";

    private static final String RUN_ARGUMENT_W = "-R";

    private static final String RUN_ARGUMENT_X = "start";

    private static final String STOP_ARGUMENT_W = "-S";

    private static final String STOP_ARGUMENT_X = "stop";

    private static final String DEFAULT_DOCKER = "docker";

    private static final String DEFAULT_MARKLOGIC_COMMAND = "MarkLogic";

    private static final String DOCKER_RUN_ARGUMENT = RUN_ARGUMENT_X;

    private static final String DOCKER_STOP_ARGUMENT = STOP_ARGUMENT_X;

    private static final String DEFAULT_MARKLOGIC_PREFIX_X = "/sbin/service";

    private static final String DEFAULT_MARKLOGIC_PREFIX_W = getEnvironmentVariable("ProgramFiles") + "\\MarkLogic\\";

    private static final String DEFAULT_MARKLOGIC_PREFIX_OSX = "~/Library/StartupItems/";

    private static final String ALIVE_URL = "http://%s:%d/admin/v1/timestamp";

    private String targetPath = DEFAULT_MARKLOGIC_TARGET_PATH;

    private Map<String, String> extraCommandArguments = new HashMap<String, String>();

    private List<String> singleCommandArguments = new ArrayList<String>();

    private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();

    private OperatingSystemResolver operatingSystemResolver = new OsNameSystemPropertyOperatingSystemResolver();

    private MarkLogicLowLevelOps markLogicLowLevelOps = MarkLogicLowLevelOpsFactory.getSingletonInstance();

    private ProcessRunnable processRunnable;

    private String dockerCommand;

    private String dockerContainer;

    private boolean starting;

    private String marklogicCommandPrefix;

    /**
     * This is the administration port and not the application's one!
     */
    private int port = 8001;

    private String adminUser = "admin";

    private String adminPassword = "admin";


    public ManagedMarkLogicLifecycleManager() {
        marklogicCommandPrefix = defaultMarklogicCommandPrefix();
    }

    public boolean isConfigured() {
        return !(marklogicCommandPrefix == null && dockerContainer == null);
    }

    @Override
    public String getHost() {
        return "localhost";
    }

    @Override
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUsername(String username) {
        adminUser = username;
    }

    public void setPassword(String password) {
        adminPassword = password;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    public String getDockerCommand() {
        return dockerCommand;
    }

    public void setDockerCommand(String dockerCommand) {
        this.dockerCommand = dockerCommand;
    }

    public String getDockerContainer() {
        return dockerContainer;
    }

    public void setDockerContainer(String dockerContainer) {
        this.dockerContainer = dockerContainer;
    }

    public String getMarklogicCommandPrefix() {
        return marklogicCommandPrefix;
    }

    public void setMarklogicCommandPrefix(String marklogicCommandPrefix) {
        this.marklogicCommandPrefix = marklogicCommandPrefix;
    }

    @Override
    public void doStart() throws Throwable {
        LOGGER.info("Starting MarkLogic instance.");
        createTargetPath();
        startMarkLogic();
        boolean isServerUp = assertThatConnectionToMarkLogicIsPossible();
        if (!isServerUp) {
            throw new IllegalStateException("Couldn't establish a connection with MarkLogic server at " + getHost() + ":" + getPort());
        }
        LOGGER.info("Started MarkLogic instance.");
    }

    @Override
    public void doStop() {
        LOGGER.info("Stopping MarkLogic instance.");
        try {
            stopMarkLogic();
            boolean isServerDown = assertThatConnectionToMarkLogicIsNotPossible();
            if (!isServerDown) {
                throw new IllegalStateException("Couldn't shut down the MarkLogic server at " + getHost() + ":" + getPort());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to stop MarkLogic instance!", e);
        }
        LOGGER.info("Stopped MarkLogic instance.");
    }

    public void addExtraCommandLineArgument(String argumentName, String argumentValue) {
        this.extraCommandArguments.put(argumentName, argumentValue);
    }

    public void addSingleCommandLineArgument(String argument) {
        this.singleCommandArguments.add(argument);
    }

    protected void setCommandLineExecutor(CommandLineExecutor commandLineExecutor) {
        this.commandLineExecutor = commandLineExecutor;
    }

    protected void setOperatingSystemResolver(
            OperatingSystemResolver operatingSystemResolver) {
        this.operatingSystemResolver = operatingSystemResolver;
    }

    protected void setMarkLogicLowLevelOps(MarkLogicLowLevelOps markLogicLowLevelOps) {
        this.markLogicLowLevelOps = markLogicLowLevelOps;
    }

    private File createTargetPath() {
        File result = new File(targetPath + separatorChar);
        if (result.exists()) {
            deleteDir(result);
        }
        result.mkdirs();
        return result;
    }

    private List<String> startMarkLogic() throws InterruptedException {
        starting = true;
        CountDownLatch processIsReady = new CountDownLatch(1);
        processRunnable = new ProcessRunnable(processIsReady);
        Thread thread = new Thread(processRunnable);
        thread.start();
        processIsReady.await();
        return processRunnable.consoleOutput;
    }

    private List<String> stopMarkLogic() throws InterruptedException {
        starting = false;
        CountDownLatch processIsReady = new CountDownLatch(1);
        processRunnable = new ProcessRunnable(processIsReady);
        Thread thread = new Thread(processRunnable);
        thread.start();
        processIsReady.await();
        return processRunnable.consoleOutput;
    }

    private List<String> buildOperationSystemProgramAndArguments() {
        List<String> programAndArguments = new ArrayList<String>(marklogicCommand());
        for (String argument : singleCommandArguments) {
            programAndArguments.add(argument);
        }
        for (String argumentName : extraCommandArguments.keySet()) {
            programAndArguments.add(argumentName);
            programAndArguments.add(extraCommandArguments.get(argumentName));
        }
        return programAndArguments;
    }

    private List<String> marklogicCommand() {
        List<String> result;
        OperatingSystem operatingSystem = operatingSystemResolver.currentOperatingSystem();
        if (dockerContainer != null && !dockerContainer.trim().isEmpty()) {
            String dc = dockerCommand == null ? DEFAULT_DOCKER : dockerCommand;
            result = asList(new String[]{dc, starting ? DOCKER_RUN_ARGUMENT : DOCKER_STOP_ARGUMENT, dockerContainer});
        } else {
            switch (operatingSystem.getFamily()) {
                case WINDOWS:
                    result = asList(new String[]{marklogicCommandPrefix + DEFAULT_MARKLOGIC_COMMAND, starting ? RUN_ARGUMENT_W : STOP_ARGUMENT_W});
                    break;
                case MAC:
                    result = asList(new String[]{marklogicCommandPrefix + DEFAULT_MARKLOGIC_COMMAND, starting ? RUN_ARGUMENT_X : STOP_ARGUMENT_X});
                    break;
                default:
                    result = asList(new String[]{marklogicCommandPrefix, DEFAULT_MARKLOGIC_COMMAND, starting ? RUN_ARGUMENT_X : STOP_ARGUMENT_X});
                    break;
            }
        }
        return result;
    }

    private String defaultMarklogicCommandPrefix() {
        OperatingSystem operatingSystem = operatingSystemResolver.currentOperatingSystem();
        switch (operatingSystem.getFamily()) {
            case WINDOWS:
                return DEFAULT_MARKLOGIC_PREFIX_W;
            case MAC:
                return DEFAULT_MARKLOGIC_PREFIX_OSX;
            default://UX
                return DEFAULT_MARKLOGIC_PREFIX_X;
        }
    }

    private boolean assertThatConnectionToMarkLogicIsPossible()
            throws InterruptedException, IOException {
        return markLogicLowLevelOps.assertThatConnectionIsPossible(getHost(), getPort(), format(ALIVE_URL, getHost(), getPort()), adminUser, adminPassword);
    }

    private boolean assertThatConnectionToMarkLogicIsNotPossible()
            throws InterruptedException, IOException {
        return markLogicLowLevelOps.assertThatConnectionIsNotPossible(getHost(), getPort(), format(ALIVE_URL, getHost(), getPort()), adminUser, adminPassword);
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
                consoleOutput = getConsoleOutput(process);
            } catch (IOException e) {
                throw prepareException(e);
            } finally {
                processIsReady.countDown();
            }

            try {
                process.waitFor();
                if (process.exitValue() != 0) {
                    LOGGER.warn(
                            "MarkLogic ["
                                    + buildOperationSystemProgramAndArguments()
                                    + "] console output is: "
                                    + consoleOutput);
                }
            } catch (InterruptedException ie) {
                throw prepareException(ie);
            }
        }

        private IllegalStateException prepareException(Exception e) {
            return new IllegalStateException(
                    "MarkLogic ["
                            + buildOperationSystemProgramAndArguments()
                            + "] could not be started. Next console message was thrown: "
                            + e.getMessage());
        }

        private Process startProcess() throws IOException {
            return commandLineExecutor.startProcessInDirectoryAndArguments(
                    targetPath, buildOperationSystemProgramAndArguments());
        }

        private List<String> getConsoleOutput(Process process) throws IOException {
            return commandLineExecutor.getConsoleOutput(process);
        }
    }
}
