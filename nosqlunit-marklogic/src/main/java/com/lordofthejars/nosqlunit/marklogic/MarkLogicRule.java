package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.MarkLogicServerException;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfigurationBuilder.marklogic;


public class MarkLogicRule extends AbstractNoSqlTestRule {

    private static final String EXTENSION = "xml";

    protected DatabaseOperation<DatabaseClient> databaseOperation;

    public MarkLogicRule(MarkLogicConfiguration marklogicConfiguration) {
        super(marklogicConfiguration.getConnectionIdentifier());
        try {
            databaseOperation = new MarkLogicOperation(marklogicConfiguration);
        } catch (MarkLogicServerException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * With JUnit 4.10 is impossible to get target from a Rule, it seems that future versions will support it. For now constructor is approach is the only way.
     */
    public MarkLogicRule(MarkLogicConfiguration marklogicConfiguration, Object target) {
        super(marklogicConfiguration.getConnectionIdentifier());
        try {
            setTarget(target);
            databaseOperation = new MarkLogicOperation(marklogicConfiguration);
        } catch (MarkLogicServerException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public DatabaseOperation<DatabaseClient> getDatabaseOperation() {
        return databaseOperation;
    }

    @Override
    public String getWorkingExtension() {
        return EXTENSION;
    }

    @Override
    public void close() {
        //databaseOperation.connectionManager().release();
    }

    public static class MarkLogicRuleBuilder {

        private MarkLogicConfiguration marklogicConfiguration;

        private Object target;

        private MarkLogicRuleBuilder() {
        }

        public static MarkLogicRuleBuilder newMarkLogicRule() {
            return new MarkLogicRuleBuilder();
        }

        public MarkLogicRuleBuilder configure(MarkLogicConfiguration marklogicConfiguration) {
            this.marklogicConfiguration = marklogicConfiguration;
            return this;
        }

        public MarkLogicRuleBuilder unitInstance(Object target) {
            this.target = target;
            return this;
        }

        public MarkLogicRule defaultManagedMarkLogic(String database) {
            return new MarkLogicRule(marklogic().database(database).build());
        }

        public MarkLogicRule defaultManagedMarkLogic(String database, int port) {
            return new MarkLogicRule(marklogic().database(database).port(port).build());
        }

        public MarkLogicRule defaultSpringMarkLogic(String database) {
            return new SpringMarkLogicRule(marklogic().database(database).build());
        }

        /**
         * We can use defaultManagedMarkLogic(String database).
         *
         * @param database
         * @param target
         * @return
         */
        @Deprecated
        public MarkLogicRule defaultManagedMarkLogic(String database, Object target) {
            return new MarkLogicRule(marklogic().database(database).build(), target);
        }

        public MarkLogicRule build() {

            if (this.marklogicConfiguration == null) {
                throw new IllegalArgumentException("Configuration object should be provided.");
            }

            return new MarkLogicRule(marklogicConfiguration, target);
        }

    }

}
