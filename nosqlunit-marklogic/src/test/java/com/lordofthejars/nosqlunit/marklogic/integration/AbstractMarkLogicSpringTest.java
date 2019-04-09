package com.lordofthejars.nosqlunit.marklogic.integration;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.DatabaseClient;
import org.junit.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicRule.MarkLogicRuleBuilder.newMarkLogicRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public abstract class AbstractMarkLogicSpringTest {

    @Rule
    public MarkLogicRule marklogicRule = newMarkLogicRule().defaultSpringMarkLogic();

    @Autowired
    protected DatabaseClient databaseClient;

    @Autowired
    private ApplicationContext applicationContext;

    protected void validateMarkLogicConnection() {
        DatabaseOperation<DatabaseClient> databaseOperation = marklogicRule.getDatabaseOperation();
        DatabaseClient connectionManager = databaseOperation.connectionManager();
        assertThat(connectionManager, is(databaseClient));
    }
}
