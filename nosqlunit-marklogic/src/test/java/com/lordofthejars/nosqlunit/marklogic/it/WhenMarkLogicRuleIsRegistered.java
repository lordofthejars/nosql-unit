package com.lordofthejars.nosqlunit.marklogic.it;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;

import static com.lordofthejars.nosqlunit.core.LoadStrategyEnum.*;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfigurationBuilder.marklogic;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class WhenMarkLogicRuleIsRegistered {

    @ClassRule
    public static ManagedMarkLogic managedMarkLogicDb = newManagedMarkLogicRule().build();

    @Test(expected = NoSqlAssertionError.class)
    public void should_fail_if_expected_data_is_non_strict_equal() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().database("test").build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        Statement noStatement = new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };
        FrameworkMethod frameworkMethod = frameworkMethod(XmlTestClass.class, "one_wrong");
        Statement marklogicStatement = managedMarkLogicRule.apply(noStatement, frameworkMethod, new XmlTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_assert_if_expected_data_is_strict_equal() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().database("test").build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        Statement noStatement = new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(XmlTestClass.class, "one_equal");
        Statement marklogicStatement = managedMarkLogicRule.apply(noStatement, frameworkMethod, new XmlTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_clean_dataset_with_delete_all_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().database("test").build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        Statement noStatement = new Statement() {
            @Override
            public void evaluate() throws Throwable {
            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(XmlTestClass.class, "one_delete");
        Statement marklogicStatement = managedMarkLogicRule.apply(noStatement, frameworkMethod, new XmlTestClass());
        marklogicStatement.evaluate();

        //DBObject currentData = findOneDBOjectByParameter("collection1", "id", 1);
        //assertThat(currentData, nullValue());
    }

    @Test
    public void should_insert_new_dataset_with_insert_strategy() throws Throwable {

        MarkLogicConfiguration marklogicConfiguration = marklogic().database("test").build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(XmlTestClass.class, "my_insert_test_1");

        XmlTestClass testObject = new XmlTestClass();
        Statement marklogicStatement = managedMarkLogicRule.apply(noStatement, frameworkMethod, testObject);
        marklogicStatement.evaluate();

        //DBObject currentData = findOneDBOjectByParameter("collection1", "id", 1);
        //assertThat((String) currentData.get("code"), is("JSON dataset"));

        FrameworkMethod frameworkMethod2 = frameworkMethod(XmlTestClass.class, "my_insert_test_2");

        Statement marklogicStatement2 = managedMarkLogicRule.apply(noStatement, frameworkMethod2, testObject);
        marklogicStatement2.evaluate();

        //DBObject previousData = findOneDBOjectByParameter("collection1", "id", 1);
        //assertThat((String) previousData.get("code"), is("JSON dataset"));

        //DBObject data = findOneDBOjectByParameter("collection3", "id", 6);
        //assertThat((String) data.get("code"), is("Another row"));
    }

    @Test
    public void should_clean_previous_data_and_insert_new_dataset_with_clean_insert_strategy() throws Throwable {

        MarkLogicConfiguration marklogicConfiguration = marklogic().database("test").build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        Statement noStatement = new Statement() {

            @Override
            public void evaluate() throws Throwable {

            }
        };

        FrameworkMethod frameworkMethod = frameworkMethod(XmlTestClass.class, "my_equal_test");

        XmlTestClass testObject = new XmlTestClass();

        Statement marklogicStatement = managedMarkLogicRule.apply(noStatement, frameworkMethod, testObject);
        marklogicStatement.evaluate();

        //DBObject currentData = findOneDBOjectByParameter("collection1", "id", 1);
        //assertThat((String) currentData.get("code"), is("JSON dataset"));

        FrameworkMethod frameworkMethod2 = frameworkMethod(XmlTestClass.class, "my_equal_test_2");

        Statement marklogicStatement2 = managedMarkLogicRule.apply(noStatement, frameworkMethod2, testObject);
        marklogicStatement2.evaluate();

        //DBObject previousData = findOneDBOjectByParameter("collection1", "id", 1);
        //assertThat(previousData, nullValue());

        //DBObject data = findOneDBOjectByParameter("collection3", "id", 6);
        //assertThat((String) data.get("code"), is("Another row"));
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

class XmlTestClass {

    @Test
    @UsingDataSet(locations = "test-one.xml", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "test-one-wrong.xml")
    public void one_wrong() {
    }

    @Test
    @UsingDataSet(locations = "test-one.xml", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "test-one-expected.xml")
    public void one_equal() {
    }

    @Test
    @UsingDataSet(locations = "test-two.xml", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "test-two-expected.xml")
    public void two_equal() {
    }

    @Test
    @UsingDataSet(locations = "test-one.xml", loadStrategy = CLEAN_INSERT)
    public void one_no_comparison() {
    }

    @Test
    @UsingDataSet(locations = "test-two.xml", loadStrategy = CLEAN_INSERT)
    public void two_no_comparison() {
    }

    @Test
    @UsingDataSet(locations = "test-one.xml", loadStrategy = DELETE_ALL)
    public void one_delete() {
    }

    @Test
    @UsingDataSet(locations = "test-two.xml", loadStrategy = DELETE_ALL)
    public void two_delete() {
    }

    @Test
    @UsingDataSet(locations = "test-one.xml", loadStrategy = INSERT)
    public void one_insert() {
    }

    @Test
    @UsingDataSet(locations = "test-two.xml", loadStrategy = INSERT)
    public void two_insert() {
    }
}
