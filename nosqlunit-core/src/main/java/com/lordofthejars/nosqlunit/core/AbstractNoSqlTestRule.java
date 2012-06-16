package com.lordofthejars.nosqlunit.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;

public abstract class AbstractNoSqlTestRule implements TestRule {

	private static final String EXPECTED_RESERVED_WORD = "-expected";

	private DefaultDataSetLocationResolver defaultDataSetLocationResolver;

	/* TODO Guice */
	private LoadStrategyFactory loadStrategyFactory = new ReflectionLoadStrategyFactory();

	public AbstractNoSqlTestRule() {
	}

	public abstract DatabaseOperation getDatabaseOperation();

	public abstract String getWorkingExtension();

	@Override
	public Statement apply(final Statement base, final Description description) {
		return new Statement() {

			@Override
			public void evaluate() throws Throwable {

				defaultDataSetLocationResolver = new DefaultDataSetLocationResolver(
						description.getTestClass());

				UsingDataSet usingDataSet = getUsingDataSetAnnotation();

				if (isTestAnnotatedWithDataSet(usingDataSet)) {
					loadDataSet(usingDataSet, description);
				}

				base.evaluate();

				ShouldMatchDataSet shouldMatchDataSet = getShouldMatchDataSetAnnotation();

				if (isTestAnnotatedWithExpectedDataSet(shouldMatchDataSet)) {
					assertExpectation(shouldMatchDataSet);
				}

			}

			private ShouldMatchDataSet getShouldMatchDataSetAnnotation() {

				ShouldMatchDataSet shouldMatchDataSet = description
						.getAnnotation(ShouldMatchDataSet.class);

				if (!isTestAnnotatedWithExpectedDataSet(shouldMatchDataSet)) {

					Class<?> testClass = description.getTestClass();
					shouldMatchDataSet = testClass
							.getAnnotation(ShouldMatchDataSet.class);

				}

				return shouldMatchDataSet;
			}

			private UsingDataSet getUsingDataSetAnnotation() {

				UsingDataSet usingDataSet = description
						.getAnnotation(UsingDataSet.class);

				if (!isTestAnnotatedWithDataSet(usingDataSet)) {

					Class<?> testClass = description.getTestClass();
					usingDataSet = testClass.getAnnotation(UsingDataSet.class);

				}

				return usingDataSet;
			}

			private void assertExpectation(ShouldMatchDataSet shouldMatchDataSet)
					throws IOException {

				String location = shouldMatchDataSet.location();
				String scriptContent = "";

				if (isNotEmptyString(location)) {
					scriptContent = IOUtils
							.readAllStreamFromClasspathBaseResource(
									defaultDataSetLocationResolver
											.getResourceBase(), location);
				} else {
					location = defaultDataSetLocationResolver
							.resolveDefaultDataSetLocation(shouldMatchDataSet,
									description, EXPECTED_RESERVED_WORD + "."
											+ getWorkingExtension());

					if (location != null) {
						scriptContent = IOUtils
								.readAllStreamFromClasspathBaseResource(
										defaultDataSetLocationResolver
												.getResourceBase(), location);
					}

				}

				if (isNotEmptyString(scriptContent)) {
					getDatabaseOperation().databaseIs(scriptContent);
				} else {
					throw new IllegalArgumentException(
							"File specified in location attribute "
									+ location
									+ " of ShouldMatchDataSet is not present, or no files matching default location.");
				}

			}

			private void loadDataSet(UsingDataSet usingDataSet,
					Description description) throws IOException {

				String[] locations = usingDataSet.locations();

				List<String> scriptContent = new ArrayList<String>();

				if (isLocationsAttributeSpecified(locations)) {

					scriptContent.addAll(IOUtils
							.readAllStreamsFromClasspathBaseResource(
									defaultDataSetLocationResolver
											.getResourceBase(), locations));

				} else {

					String location = defaultDataSetLocationResolver
							.resolveDefaultDataSetLocation(usingDataSet,
									description, "." + getWorkingExtension());

					if (location != null) {
						scriptContent.add(IOUtils
								.readAllStreamFromClasspathBaseResource(
										defaultDataSetLocationResolver
												.getResourceBase(), location));
					}

				}

				LoadStrategyEnum loadStrategyEnum = usingDataSet.loadStrategy();
				LoadStrategyOperation loadStrategyOperation = loadStrategyFactory
						.getLoadStrategyInstance(loadStrategyEnum,
								getDatabaseOperation());
				loadStrategyOperation.executeScripts(scriptContent
						.toArray(new String[scriptContent.size()]));

			}

			private boolean isNotEmptyString(String location) {
				return location != null && !"".equals(location.trim());
			}

			private boolean isLocationsAttributeSpecified(String[] locations) {
				return locations != null && locations.length > 0;
			}

			private boolean isTestAnnotatedWithExpectedDataSet(
					ShouldMatchDataSet shouldMatchDataSet) {
				return shouldMatchDataSet != null;
			}

			private boolean isTestAnnotatedWithDataSet(UsingDataSet usingDataSet) {
				return usingDataSet != null;
			}
		};
	}

	public void setLoadStrategyFactory(LoadStrategyFactory loadStrategyFactory) {
		this.loadStrategyFactory = loadStrategyFactory;
	}

}
