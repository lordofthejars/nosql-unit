package com.lordofthejars.nosqlunit.marklogic.integration;

import com.lordofthejars.nosqlunit.annotation.CustomComparisonStrategy;
import com.lordofthejars.nosqlunit.annotation.IgnorePropertyValue;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicFlexibleComparisonStrategy;
import com.lordofthejars.nosqlunit.marklogic.MarkLogicRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static com.lordofthejars.nosqlunit.marklogic.ManagedMarkLogic.MarkLogicServerRuleBuilder.newManagedMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.MarkLogicRule.MarkLogicRuleBuilder.newMarkLogicRule;
import static com.lordofthejars.nosqlunit.marklogic.ml.DefaultMarkLogic.PROPERTIES;

/**
 * Tests flexible comparison by ignoring some properties in the actual result sets
 * (e.g. for 'similar' equality).
 */
@CustomComparisonStrategy(comparisonStrategy = MarkLogicFlexibleComparisonStrategy.class)
public class MarkLogicFlexibleComparisonStrategyTest {

    @ClassRule
    public static final ManagedMarkLogic managedMarkLogic = newManagedMarkLogicRule().build();

    @Rule
    public MarkLogicRule MarkLogicDbRule = newMarkLogicRule().defaultManagedMarkLogic(PROPERTIES.contentDatabase, PROPERTIES.appPort);

    @Test
    @UsingDataSet(locations = "jane-john.xml")
    @ShouldMatchDataSet(location = "jane-john-ignored.xml")
    @IgnorePropertyValue(properties = {"//phoneNumber/type", "/person/age", "//address/@type"})
    public void shouldIgnoreXmlPropertiesInFlexibleStrategy() {
    }

    @Test
    @UsingDataSet(locations = "jane-john.json")
    @ShouldMatchDataSet(location = "jane-john-ignored.json")
    @IgnorePropertyValue(properties = {"phoneNumbers[*].type", "age"})
    public void shouldIgnoreJsonPropertiesInFlexibleStrategy() {
    }
}
