package com.lordofthejars.nosqlunit.marklogic;

import org.junit.rules.ExternalResource;

/**
 * Runs a MarkLogic server before each test suite.
 */
public class ManagedMarkLogic extends ExternalResource {


    protected ManagedMarkLogicLifecycleManager managedMarkLogicLifecycleManager;

    private ManagedMarkLogic() {
    }

    @Override
    public void before() throws Throwable {
        managedMarkLogicLifecycleManager.startEngine();
    }

    @Override
    public void after() {
        managedMarkLogicLifecycleManager.stopEngine();
    }

    /**
     * Builder to start MarkLogic server accordingly to your setup
     */
    public static class MarkLogicServerRuleBuilder {

        private ManagedMarkLogicLifecycleManager managedMarkLogicLifecycleManager;

        private MarkLogicServerRuleBuilder() {
            managedMarkLogicLifecycleManager = new ManagedMarkLogicLifecycleManager();
        }

        public static MarkLogicServerRuleBuilder newManagedMarkLogicRule() {
            return new MarkLogicServerRuleBuilder();
        }

        public MarkLogicServerRuleBuilder username(String username) {
            managedMarkLogicLifecycleManager.setUsername(username);
            return this;
        }

        public MarkLogicServerRuleBuilder password(String password) {
            managedMarkLogicLifecycleManager.setPassword(password);
            return this;
        }


        public MarkLogicServerRuleBuilder dockerCommand(String dockerCommand) {
            managedMarkLogicLifecycleManager.setDockerCommand(dockerCommand);
            return this;
        }

        public MarkLogicServerRuleBuilder dockerContainer(String dockerContainer) {
            managedMarkLogicLifecycleManager.setDockerContainer(dockerContainer);
            return this;
        }

        public MarkLogicServerRuleBuilder marklogicPrefix(String marklogicPath) {
            managedMarkLogicLifecycleManager.setMarklogicCommandPrefix(marklogicPath);
            return this;
        }

        public MarkLogicServerRuleBuilder port(int port) {
            managedMarkLogicLifecycleManager.setPort(port);
            return this;
        }

        public MarkLogicServerRuleBuilder targetPath(String targetPath) {
            managedMarkLogicLifecycleManager.setTargetPath(targetPath);
            return this;
        }

        public MarkLogicServerRuleBuilder appendCommandLineArguments(
                String argumentName, String argumentValue) {
            managedMarkLogicLifecycleManager.addExtraCommandLineArgument(argumentName,
                    argumentValue);
            return this;
        }

        public MarkLogicServerRuleBuilder appendSingleCommandLineArguments(
                String argument) {
            managedMarkLogicLifecycleManager.addSingleCommandLineArgument(argument);
            return this;
        }

        public ManagedMarkLogic build() {
            if (!managedMarkLogicLifecycleManager.isConfigured()) {
                throw new IllegalArgumentException("The MarkLogic lifecycle is not configured!");
            }
            ManagedMarkLogic managedMarkLogic = new ManagedMarkLogic();
            managedMarkLogic.managedMarkLogicLifecycleManager = managedMarkLogicLifecycleManager;
            return managedMarkLogic;
        }
    }
}
