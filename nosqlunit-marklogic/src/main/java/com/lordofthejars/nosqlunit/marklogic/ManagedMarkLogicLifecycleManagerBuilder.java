package com.lordofthejars.nosqlunit.marklogic;


public class ManagedMarkLogicLifecycleManagerBuilder {

    private ManagedMarkLogicLifecycleManager managedMarkLogicLifecycleManager;

    private ManagedMarkLogicLifecycleManagerBuilder() {
        managedMarkLogicLifecycleManager = new ManagedMarkLogicLifecycleManager();
    }

    public static ManagedMarkLogicLifecycleManagerBuilder newManagedMarkLogicLifecycle() {
        return new ManagedMarkLogicLifecycleManagerBuilder();
    }

    public ManagedMarkLogicLifecycleManagerBuilder marklogicPath(String marklogicPath) {
        managedMarkLogicLifecycleManager.setMarklogicCommandPrefix(marklogicPath);
        return this;
    }

    public ManagedMarkLogicLifecycleManagerBuilder dockerCommand(String dockerCommand) {
        managedMarkLogicLifecycleManager.setDockerCommand(dockerCommand);
        return this;
    }

    public ManagedMarkLogicLifecycleManagerBuilder dockerContainer(String dockerContainer) {
        managedMarkLogicLifecycleManager.setDockerContainer(dockerContainer);
        return this;
    }

    public ManagedMarkLogicLifecycleManagerBuilder adminPort(int port) {
        managedMarkLogicLifecycleManager.setPort(port);
        return this;
    }

    public ManagedMarkLogicLifecycleManagerBuilder targetPath(String targetPath) {
        managedMarkLogicLifecycleManager.setTargetPath(targetPath);
        return this;
    }

    public ManagedMarkLogicLifecycleManagerBuilder appendCommandLineArguments(
            String argumentName, String argumentValue) {
        managedMarkLogicLifecycleManager.addExtraCommandLineArgument(argumentName,
                argumentValue);
        return this;
    }

    public ManagedMarkLogicLifecycleManagerBuilder appendSingleCommandLineArguments(
            String argument) {
        managedMarkLogicLifecycleManager.addSingleCommandLineArgument(argument);
        return this;
    }


    public ManagedMarkLogicLifecycleManager get() {
        if (!managedMarkLogicLifecycleManager.isConfigured()) {
            throw new IllegalArgumentException("The MarkLogic lifecycle is not configured!");
        }
        return managedMarkLogicLifecycleManager;
    }
}
