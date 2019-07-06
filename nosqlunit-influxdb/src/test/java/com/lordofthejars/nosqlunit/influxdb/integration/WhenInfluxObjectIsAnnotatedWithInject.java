
package com.lordofthejars.nosqlunit.influxdb.integration;

import static com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDb.InMemoryInfluxRuleBuilder.*;
import static com.lordofthejars.nosqlunit.influxdb.InfluxDbConfigurationBuilder.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;

import javax.inject.Inject;

import org.influxdb.InfluxDB;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.influxdb.InMemoryInfluxDb;
import com.lordofthejars.nosqlunit.influxdb.InfluxDbConfiguration;
import com.lordofthejars.nosqlunit.influxdb.InfluxDbRule;

public class WhenInfluxObjectIsAnnotatedWithInject {

    private static final String DB_NAME = "test-db";

    @ClassRule
    public static final InMemoryInfluxDb IN_MEMORY_DYNAMO_DB = newInMemoryInfluxDbRule().build();

    @Inject
    private InfluxDB dynamo;

    @Before
    public void setUp() {
        dynamo = null;
    }

    @Test
    public void dynamo_instance_used_in_rule_should_be_injected() throws Throwable {

        InfluxDbConfiguration dynamoDbConfiguration = influxDb().databaseName(DB_NAME).build();
        InfluxDbRule remoteInfluxDbRule = new InfluxDbRule(dynamoDbConfiguration, this);

        Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(WhenInfluxObjectIsAnnotatedWithInject.class,
                "dynamo_instance_used_in_rule_should_be_injected");
        Statement dynamodbStatement = remoteInfluxDbRule.apply(noStatement, frameworkMethod, this);
        dynamodbStatement.evaluate();

        assertThat(dynamo, is(remoteInfluxDbRule.getDatabaseOperation().connectionManager()));

    }

    @Test
    public void dynamo_instance_used_in_rule_should_be_injected_without_this_reference() throws Throwable {

        InfluxDbConfiguration dynamoDbConfiguration = influxDb().databaseName(DB_NAME).build();
        InfluxDbRule remoteInfluxDbRule = new InfluxDbRule(dynamoDbConfiguration);

        Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(WhenInfluxObjectIsAnnotatedWithInject.class,
                "dynamo_instance_used_in_rule_should_be_injected");
        Statement dynamodbStatement = remoteInfluxDbRule.apply(noStatement, frameworkMethod, this);
        dynamodbStatement.evaluate();

        assertThat(dynamo, is(remoteInfluxDbRule.getDatabaseOperation().connectionManager()));

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
