package com.lordofthejars.nosqlunit.marklogic;

import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.MarkLogicServerException;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicConfigurationBuilder.marklogic;
import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_APP_PORT;


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

        public MarkLogicRule defaultManagedMarkLogic(String host, int port) {
            return new MarkLogicRule(marklogic().database(host).port(port).build());
        }

        public MarkLogicRule defaultManagedMarkLogic(String host, int port, String database) {
            return new MarkLogicRule(marklogic().database(host).port(port).database(database).build());
        }

        public MarkLogicRule defaultSpringMarkLogic() {
            //the port is just a fake since we'll use another client in apply
            return new SpringMarkLogicRule(marklogic().port(DEFAULT_APP_PORT).build());
        }

        public MarkLogicRule build() {
            if (this.marklogicConfiguration == null) {
                throw new IllegalArgumentException("Configuration object should be provided.");
            }
            return new MarkLogicRule(marklogicConfiguration, target);
        }
    }
}
