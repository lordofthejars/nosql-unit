package com.lordofthejars.nosqlunit.marklogic.integration;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.query.ExtractedItem;
import com.marklogic.client.query.ExtractedResult;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;
import java.util.Optional;

import static com.lordofthejars.nosqlunit.core.LoadStrategyEnum.*;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogicConfigurationBuilder.marklogic;
import static com.lordofthejars.nosqlunit.marklogic.ml.MarkLogicQuery.findOneByTerm;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

/**
 * Tests JSON content handling.
 */
public class WhenMarkLogicRuleIsRegisteredThenJson {

    /**
     * The application database available in the default installation.
     */
    private static final String TEST_DATABASE = "Documents";

    /**
     * The application port available in the default installation.
     */
    private static final int TEST_APP_PORT = 8000;

    private static final Statement NO_OP_STATEMENT = new Statement() {
        @Override
        public void evaluate() throws Throwable {
        }
    };

    @ClassRule
    public static ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();


    @Test(expected = NoSqlAssertionError.class)
    public void should_fail_if_expected_data_is_non_strict_equal() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(JsonTestClass.class, "one_wrong");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new JsonTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_assert_if_expected_data_is_strict_equal() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(JsonTestClass.class, "one_equal");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new JsonTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_assert_if_expected_multi_data_is_strict_equal() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(JsonTestClass.class, "two_equal");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new JsonTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_clean_dataset_with_delete_all_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(JsonTestClass.class, "one_delete");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new JsonTestClass());
        marklogicStatement.evaluate();

        Optional<ExtractedResult> currentData = findOneByTerm(marklogicConfiguration.getDatabaseClient(), "Jane");
        assertFalse(currentData.isPresent());
    }

    @Test
    public void should_insert_new_dataset_with_insert_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        JsonTestClass testObject = new JsonTestClass();
        FrameworkMethod frameworkMethod = frameworkMethod(JsonTestClass.class, "one_insert");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, testObject);
        marklogicStatement.evaluate();

        Optional<ExtractedResult> currentData = findOneByTerm(marklogicConfiguration.getDatabaseClient(), "Jane");
        assertTrue(currentData.isPresent());
        assertEquals(1, currentData.get().size());
        ExtractedItem currentItem = currentData.get().next();
        assertNotNull(currentItem);
        assertThat(currentItem.getAs(String.class), containsString("Jane"));

        FrameworkMethod frameworkMethod2 = frameworkMethod(JsonTestClass.class, "two_insert");

        Statement marklogicStatement2 = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod2, testObject);
        marklogicStatement2.evaluate();

        Optional<ExtractedResult> previousData = findOneByTerm(marklogicConfiguration.getDatabaseClient(), "Jane");
        assertTrue(previousData.isPresent());
        assertEquals(1, previousData.get().size());
        ExtractedItem previousItem = previousData.get().next();
        assertNotNull(previousItem);
        assertThat(previousItem.getAs(String.class), containsString("Jane"));

        Optional<ExtractedResult> data = findOneByTerm(marklogicConfiguration.getDatabaseClient(), "John");
        assertTrue(data.isPresent());
        assertEquals(1, data.get().size());
        ExtractedItem item = data.get().next();
        assertNotNull(item);
        assertThat(item.getAs(String.class), containsString("John"));
    }

    @Test
    public void should_clean_previous_data_and_insert_new_dataset_with_clean_insert_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);
        JsonTestClass testObject = new JsonTestClass();

        FrameworkMethod frameworkMethod = frameworkMethod(JsonTestClass.class, "one_equal");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, testObject);
        marklogicStatement.evaluate();

        Optional<ExtractedResult> currentData = findOneByTerm(marklogicConfiguration.getDatabaseClient(), "Doe");
        assertTrue(currentData.isPresent());
        assertEquals(1, currentData.get().size());
        ExtractedItem currentItem = currentData.get().next();
        assertNotNull(currentItem);
        assertThat(currentItem.getAs(String.class), containsString("Jane"));

        FrameworkMethod frameworkMethod2 = frameworkMethod(JsonTestClass.class, "two_insert");

        Statement marklogicStatement2 = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod2, testObject);
        marklogicStatement2.evaluate();

        Optional<ExtractedResult> previousData = findOneByTerm(marklogicConfiguration.getDatabaseClient(), "Jane");
        assertTrue(previousData.isPresent());
        assertEquals(1, previousData.get().size());
        ExtractedItem previousItem = previousData.get().next();
        assertNotNull(previousItem);
        assertThat(previousItem.getAs(String.class), containsString("Jane"));

        Optional<ExtractedResult> data = findOneByTerm(marklogicConfiguration.getDatabaseClient(), "John");
        assertTrue(data.isPresent());
        assertEquals(1, data.get().size());
        ExtractedItem item = data.get().next();
        assertNotNull(item);
        assertThat(item.getAs(String.class), containsString("John"));
    }

    private FrameworkMethod frameworkMethod(Class<?> testClass, String methodName) {
        try {
            Method method = testClass.getMethod(methodName);
            return new FrameworkMethod(method);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}

class JsonTestClass {

    @Test
    @UsingDataSet(locations = "test-one.json", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "test-one-wrong.json")
    public void one_wrong() {
    }

    @Test
    @UsingDataSet(locations = "test-one.json", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "test-one-expected.json")
    public void one_equal() {
    }

    @Test
    @UsingDataSet(locations = "test-two.json", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "test-two-expected.json")
    public void two_equal() {
    }

    @Test
    @UsingDataSet(locations = "test-one.json", loadStrategy = DELETE_ALL)
    public void one_delete() {
    }

    @Test
    @UsingDataSet(locations = "test-one.json", loadStrategy = INSERT)
    public void one_insert() {
    }

    @Test
    @UsingDataSet(locations = "test-two.json", loadStrategy = INSERT)
    public void two_insert() {
    }
}
