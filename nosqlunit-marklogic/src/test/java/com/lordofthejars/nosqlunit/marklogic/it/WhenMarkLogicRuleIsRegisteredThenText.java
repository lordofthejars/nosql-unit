package com.lordofthejars.nosqlunit.marklogic.it;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.*;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;
import java.util.Optional;

import static com.lordofthejars.nosqlunit.core.LoadStrategyEnum.*;
import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfigurationBuilder.marklogic;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

/**
 * Tests plain text content handling.
 */
public class WhenMarkLogicRuleIsRegisteredThenText {

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

        FrameworkMethod frameworkMethod = frameworkMethod(TextTestClass.class, "one_wrong");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new TextTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_assert_if_expected_data_is_strict_equal() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(TextTestClass.class, "one_equal");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new TextTestClass());
        marklogicStatement.evaluate();
    }

    @Test
    public void should_clean_dataset_with_delete_all_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        FrameworkMethod frameworkMethod = frameworkMethod(TextTestClass.class, "one_delete");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, new TextTestClass());
        marklogicStatement.evaluate();

        Optional<ExtractedResult> currentData = findOptionalOneByTerm(marklogicConfiguration.getDatabaseClient(), "Jane");
        assertFalse(currentData.isPresent());
    }

    @Test
    public void should_insert_new_dataset_with_insert_strategy() throws Throwable {
        MarkLogicConfiguration marklogicConfiguration = marklogic().port(TEST_APP_PORT).database(TEST_DATABASE).build();
        MarkLogicRule managedMarkLogicRule = new MarkLogicRule(marklogicConfiguration);

        TextTestClass testObject = new TextTestClass();
        FrameworkMethod frameworkMethod = frameworkMethod(TextTestClass.class, "one_insert");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, testObject);
        marklogicStatement.evaluate();

        Optional<ExtractedResult> currentData = findOptionalOneByTerm(marklogicConfiguration.getDatabaseClient(), "Jane");
        assertTrue(currentData.isPresent());
        assertEquals(1, currentData.get().size());
        ExtractedItem currentItem = currentData.get().next();
        assertNotNull(currentItem);
        assertThat(currentItem.getAs(String.class), containsString("Jane"));

        FrameworkMethod frameworkMethod2 = frameworkMethod(TextTestClass.class, "two_insert");

        Statement marklogicStatement2 = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod2, testObject);
        marklogicStatement2.evaluate();

        Optional<ExtractedResult> previousData = findOptionalOneByTerm(marklogicConfiguration.getDatabaseClient(), "Jane");
        assertTrue(previousData.isPresent());
        assertEquals(1, previousData.get().size());
        ExtractedItem previousItem = previousData.get().next();
        assertNotNull(previousItem);
        assertThat(previousItem.getAs(String.class), containsString("Jane"));

        Optional<ExtractedResult> data = findOptionalOneByTerm(marklogicConfiguration.getDatabaseClient(), "John");
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
        TextTestClass testObject = new TextTestClass();

        FrameworkMethod frameworkMethod = frameworkMethod(TextTestClass.class, "one_equal");
        Statement marklogicStatement = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod, testObject);
        marklogicStatement.evaluate();

        Optional<ExtractedResult> currentData = findOptionalOneByTerm(marklogicConfiguration.getDatabaseClient(), "Doe");
        assertTrue(currentData.isPresent());
        assertEquals(1, currentData.get().size());
        ExtractedItem currentItem = currentData.get().next();
        assertNotNull(currentItem);
        assertThat(currentItem.getAs(String.class), containsString("Jane"));

        FrameworkMethod frameworkMethod2 = frameworkMethod(TextTestClass.class, "two_insert");

        Statement marklogicStatement2 = managedMarkLogicRule.apply(NO_OP_STATEMENT, frameworkMethod2, testObject);
        marklogicStatement2.evaluate();

        Optional<ExtractedResult> previousData = findOptionalOneByTerm(marklogicConfiguration.getDatabaseClient(), "Jane");
        assertTrue(previousData.isPresent());
        assertEquals(1, previousData.get().size());
        ExtractedItem previousItem = previousData.get().next();
        assertNotNull(previousItem);
        assertThat(previousItem.getAs(String.class), containsString("Jane"));

        Optional<ExtractedResult> data = findOptionalOneByTerm(marklogicConfiguration.getDatabaseClient(), "John");
        assertTrue(data.isPresent());
        assertEquals(1, data.get().size());
        ExtractedItem item = data.get().next();
        assertNotNull(item);
        assertThat(item.getAs(String.class), containsString("John"));
    }

    private long countByTerm(MarkLogicConfiguration marklogicConfiguration, String value) {
        DatabaseClient client = marklogicConfiguration.getDatabaseClient();
        QueryManager queryManager = client.newQueryManager();
        StringQueryDefinition query = queryManager.newStringDefinition();
        query.setDirectory("/");
        query.setCriteria(value);
        SearchHandle result = queryManager.search(query, new SearchHandle());
        assertNotNull(result);
        return result.getTotalResults();
    }

    private Optional<ExtractedResult> findOptionalOneByTerm(DatabaseClient client, String value) {
        SearchHandle handle = findOneByTerm(client, value);
        ExtractedResult result = null;
        if (handle != null && handle.getMatchResults() != null && handle.getMatchResults().length > 0) {
            result = handle.getMatchResults()[0].getExtracted();
        }
        return Optional.ofNullable(result);
    }

    private SearchHandle findOneByTerm(DatabaseClient client, String value) {
        QueryManager queryManager = client.newQueryManager();
        queryManager.setPageLength(1);
        RawCombinedQueryDefinition query = queryManager.newRawCombinedQueryDefinition(new StringHandle(
                        "<search xmlns='http://marklogic.com/appservices/search'>" +
                                "    <options>" +
                                "        <extract-document-data selected='all'>" +
                                "            <extract-path>/*</extract-path>" +
                                "        </extract-document-data>" +
                                "    </options>" +
                                "    <query>" +
                                "        <term-query>" +
                                "            <text>" + value + "</text>" +
                                "        </term-query>" +
                                "    </query>" +
                                "</search>"
                )
        );
        query.setDirectory("/");
        return queryManager.search(query, new SearchHandle());
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

class TextTestClass {

    @Test
    @UsingDataSet(locations = "person/wrong/test-one.txt", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "person/wrong/test-one-expected.txt")
    public void one_wrong() {
    }

    @Test
    @UsingDataSet(locations = "person/test-one.txt", loadStrategy = CLEAN_INSERT)
    @ShouldMatchDataSet(location = "person/test-one-expected.txt")
    public void one_equal() {
    }

    @Test
    @UsingDataSet(locations = "person/test-one.txt", loadStrategy = DELETE_ALL)
    public void one_delete() {
    }

    @Test
    @UsingDataSet(locations = "person/test-one.txt", loadStrategy = INSERT)
    public void one_insert() {
    }

    @Test
    @UsingDataSet(locations = "person/test-two.txt", loadStrategy = INSERT)
    public void two_insert() {
    }
}
