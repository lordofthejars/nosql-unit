package com.lordofthejars.nosqlunit.couchdb;

import org.junit.rules.ExternalResource;

public class ManagedCouchDb extends ExternalResource {

    protected ManagedCouchDbLifecycleManager managedCouchDbLifecycleManager;

    private ManagedCouchDb() {
        super();
    }

    public static class ManagedCouchDbRuleBuilder {

        private ManagedCouchDbLifecycleManager managedCouchDbLifecycleManager;

        private ManagedCouchDbRuleBuilder() {
            this.managedCouchDbLifecycleManager = new ManagedCouchDbLifecycleManager();
        }

        public static ManagedCouchDbRuleBuilder newManagedCouchDbRule() {
            return new ManagedCouchDbRuleBuilder();
        }

        public ManagedCouchDbRuleBuilder port(int port) {
            this.managedCouchDbLifecycleManager.setPort(port);
            return this;
        }

        public ManagedCouchDbRuleBuilder targetPath(String targetPath) {
            this.managedCouchDbLifecycleManager.setTargetPath(targetPath);
            return this;
        }

        public ManagedCouchDbRuleBuilder couchDbPath(String couchPath) {
            this.managedCouchDbLifecycleManager.setCouchDbPath(couchPath);
            return this;
        }

        public ManagedCouchDbRuleBuilder appendCommandLineArguments(String argumentName, String argumentValue) {
            this.managedCouchDbLifecycleManager.addExtraCommandLineArgument(argumentName, argumentValue);
            return this;
        }

        public ManagedCouchDbRuleBuilder appendSingleCommandLineArguments(String argument) {
            this.managedCouchDbLifecycleManager.addSingleCommandLineArgument(argument);
            return this;
        }

        public ManagedCouchDb build() {

            if (this.managedCouchDbLifecycleManager.getCouchDbPath() == null) {
                throw new IllegalArgumentException("CouchDb Path cannot be null.");
            }

            ManagedCouchDb managedCouchDb = new ManagedCouchDb();
            managedCouchDb.managedCouchDbLifecycleManager = this.managedCouchDbLifecycleManager;

            return managedCouchDb;
        }
    }

    @Override
    protected void before() throws Throwable {
        this.managedCouchDbLifecycleManager.startEngine();
    }

    @Override
    protected void after() {
        this.managedCouchDbLifecycleManager.stopEngine();
    }
}
