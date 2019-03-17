package com.lordofthejars.nosqlunit.marklogic.it;

import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;

public class WhenManagedMarkLogicRuleIsRegistered {

    @Test
    public void marklogic_server_should_start_and_stop_from_default_location() throws Throwable {
        ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();
        Statement noStatement = new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };
        Statement decotedStatement = managedMarkLogic.apply(noStatement, Description.EMPTY);
        decotedStatement.evaluate();
    }

    @Test
    public void marklogic_server_should_start_and_stop_in_docker() throws Throwable {
        ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().dockerContainer("marklogic").build();
        Statement noStatement = new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };
        Statement decotedStatement = managedMarkLogic.apply(noStatement, Description.EMPTY);
        decotedStatement.evaluate();
    }


    @Test(expected = IllegalArgumentException.class)
    public void marklogic_server_should_throw_an_exception_if_neither_service_nor_docker_container_are_set() throws Throwable {
        ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().marklogicPrefix(null).dockerContainer(null).build();
        Statement noStatement = new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };
        Statement decotedStatement = managedMarkLogic.apply(noStatement, Description.EMPTY);
        decotedStatement.evaluate();
    }
}
