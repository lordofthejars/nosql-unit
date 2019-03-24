package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.MarkLogicServerException;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicConfigurationBuilder.marklogic;


public class MarkLogicRule extends AbstractNoSqlTestRule {

    public static final String EXPECTED_RESERVED_WORD = "-expected";

    private static final String DEFAULT_EXTENSION = "xml";

    protected MarkLogicOperation databaseOperation;

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
            databaseOperation = new MarkLogicOperation(marklogicConfiguration, target);
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
        return DEFAULT_EXTENSION;
    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object testObject) {
        databaseOperation.setTarget(testObject);
        return super.apply(base, method, testObject);
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

        public MarkLogicRule defaultManagedMarkLogic(String database, int port) {
            return new MarkLogicRule(marklogic().database(database).port(port).build());
        }

        public MarkLogicRule defaultSpringMarkLogic(String database, int port) {
            return new SpringMarkLogicRule(marklogic().database(database).port(port).build());
        }

        public MarkLogicRule build() {
            if (this.marklogicConfiguration == null) {
                throw new IllegalArgumentException("Configuration object should be provided.");
            }
            return new MarkLogicRule(marklogicConfiguration, target);
        }
    }
}
