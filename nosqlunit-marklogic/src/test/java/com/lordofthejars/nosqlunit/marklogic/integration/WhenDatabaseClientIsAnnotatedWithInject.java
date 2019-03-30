package com.lordofthejars.nosqlunit.marklogic.integration;


import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.DatabaseClient;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import javax.inject.Inject;
import java.lang.reflect.Method;

import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicConfigurationBuilder.marklogic;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class WhenDatabaseClientIsAnnotatedWithInject {

    @ClassRule
    public static ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();

    @Inject
    private DatabaseClient databaseClient;

    @Before
    public void setUp() {
        databaseClient = null;
    }

    @Test
    public void database_client_used_in_rule_should_be_injected() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().build();
        MarkLogicRule remoteMarkLogicRule = new MarkLogicRule(marklogicConfiguration, this);

        Statement noStatement = new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(WhenDatabaseClientIsAnnotatedWithInject.class, "database_client_used_in_rule_should_be_injected");
        Statement marklogicdbStatement = remoteMarkLogicRule.apply(noStatement, frameworkMethod, this);
        marklogicdbStatement.evaluate();

        assertThat(databaseClient, is(remoteMarkLogicRule.getDatabaseOperation().connectionManager()));

    }

    @Test
    public void database_client_used_in_rule_should_be_injected_without_this_reference() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().build();
        MarkLogicRule remoteMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        Statement noStatement = new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(WhenDatabaseClientIsAnnotatedWithInject.class, "database_client_used_in_rule_should_be_injected");
        Statement marklogicdbStatement = remoteMarkLogicRule.apply(noStatement, frameworkMethod, this);
        marklogicdbStatement.evaluate();

        assertThat(databaseClient, is(remoteMarkLogicRule.getDatabaseOperation().connectionManager()));
    }

    private FrameworkMethod frameworkMethod(Class<?> testClass, String methodName) {
        try {
            Method method = testClass.getMethod(methodName);
            return new FrameworkMethod(method);
        } catch (SecurityException e) {
            throw new IllegalArgumentException(e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
