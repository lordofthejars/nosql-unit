package com.lordofthejars.nosqlunit.core;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectFirst;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.annotation.Selective;
import com.lordofthejars.nosqlunit.annotation.SelectiveMatcher;
import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;

public abstract class AbstractNoSqlTestRule implements TestRule {

	private static final String EXPECTED_RESERVED_WORD = "-expected";

	/**
	 * With JUnit 4.10 is impossible to get target from a Rule, it seems that
	 * future versions will support it. For now constructor is apporach is the
	 * only way.
	 */
	private Object target;

	private String identifier;

	private DefaultDataSetLocationResolver defaultDataSetLocationResolver;

	private LoadStrategyFactory loadStrategyFactory = new ReflectionLoadStrategyFactory();

	private InjectAnnotationProcessor injectAnnotationProcessor;

	public AbstractNoSqlTestRule(String identifier) {
		this.identifier = identifier;
		this.injectAnnotationProcessor = new InjectAnnotationProcessor(
				this.identifier);
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

				injectAnnotationProcessor.processInjectAnnotation(
						description.getTestClass(), target,
						getDatabaseOperation().connectionManager());

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

				String scriptContent = loadExpectedContentScript(description,
						shouldMatchDataSet);

				if (isNotEmptyString(scriptContent)) {
					getDatabaseOperation().databaseIs(scriptContent);
				} else {
					throw new IllegalArgumentException(
							"File specified in location or selective matcher attribute "
									+ " of ShouldMatchDataSet is not present, or no files matching default location.");
				}

			}

			private String loadExpectedContentScript(
					final Description description,
					ShouldMatchDataSet shouldMatchDataSet) throws IOException {
				String location = shouldMatchDataSet.location();
				String scriptContent = "";

				if (isNotEmptyString(location)) {
					scriptContent = loadExpectedResultFromLocationAttribute(location);
				} else {

					SelectiveMatcher[] selectiveMatchers = shouldMatchDataSet
							.withSelectiveMatcher();

					SelectiveMatcher requiredSelectiveMatcher = findSelectiveMatcherByConnectionIdentifier(selectiveMatchers);
					
					if (isSelectiveMatchersDefined(requiredSelectiveMatcher)) {

						scriptContent = loadExpectedResultFromLocationAttribute(requiredSelectiveMatcher.location());
						
					} else {
						scriptContent = loadExpectedResultFromDefaultLocation(
								description, shouldMatchDataSet, scriptContent);
					}
				}
				return scriptContent;
			}

			private boolean isSelectiveMatchersDefined(
					SelectiveMatcher requiredSelectiveMatcher) {
				return requiredSelectiveMatcher != null;
			}

			private SelectiveMatcher findSelectiveMatcherByConnectionIdentifier(SelectiveMatcher[] selectiveMatchers) {
				return selectFirst(selectiveMatchers, 
						having(on(SelectiveMatcher.class).identifier(), equalTo(identifier)).and(
						having(on(SelectiveMatcher.class).location(), notNullValue())));
			}
			
			private String loadExpectedResultFromDefaultLocation(
					final Description description,
					ShouldMatchDataSet shouldMatchDataSet, String scriptContent)
					throws IOException {
				String defaultLocation = defaultDataSetLocationResolver
						.resolveDefaultDataSetLocation(
								shouldMatchDataSet, description,
								EXPECTED_RESERVED_WORD + "."
										+ getWorkingExtension());

				if (defaultLocation != null) {
					scriptContent = loadExpectedResultFromLocationAttribute(defaultLocation);
				}
				return scriptContent;
			}

			private String loadExpectedResultFromLocationAttribute(
					String location) throws IOException {
				String scriptContent;
				scriptContent = IOUtils.readAllStreamFromClasspathBaseResource(
						defaultDataSetLocationResolver.getResourceBase(),
						location);
				return scriptContent;
			}

			private void loadDataSet(UsingDataSet usingDataSet,
					Description description) throws IOException {

				String[] locations = usingDataSet.locations();

				List<String> scriptContent = new ArrayList<String>();

				scriptContent.addAll(loadGlobalDataSets(usingDataSet,
						description, locations));
				scriptContent.addAll(loadSelectiveDataSets(usingDataSet));

				LoadStrategyEnum loadStrategyEnum = usingDataSet.loadStrategy();
				LoadStrategyOperation loadStrategyOperation = loadStrategyFactory
						.getLoadStrategyInstance(loadStrategyEnum,
								getDatabaseOperation());
				loadStrategyOperation.executeScripts(scriptContent
						.toArray(new String[scriptContent.size()]));

			}

			private List<String> loadSelectiveDataSets(UsingDataSet usingDataSet)
					throws IOException {

				List<String> scriptContent = new ArrayList<String>();

				if (isSelectiveLocationsAttributeSpecified(usingDataSet)) {
					Selective[] selectiveLocations = usingDataSet
							.withSelectiveLocations();
					if (selectiveLocations != null
							&& selectiveLocations.length > 0) {
						for (Selective selective : selectiveLocations) {
							if (identifier
									.equals(selective.identifier().trim())
									&& isLocationsAttributeSpecified(selective
											.locations())) {
								scriptContent
										.addAll(IOUtils
												.readAllStreamsFromClasspathBaseResource(
														defaultDataSetLocationResolver
																.getResourceBase(),
														selective.locations()));
							}
						}
					}
				}

				return scriptContent;
			}

			private List<String> loadGlobalDataSets(UsingDataSet usingDataSet,
					Description description, String[] locations)
					throws IOException {

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

				return scriptContent;
			}

			private boolean isSelectiveLocationsAttributeSpecified(
					UsingDataSet usingDataSet) {
				Selective[] selectiveLocations = usingDataSet
						.withSelectiveLocations();
				if (selectiveLocations != null && selectiveLocations.length > 0) {
					for (Selective selective : selectiveLocations) {
						if (identifier.equals(selective.identifier().trim())
								&& isLocationsAttributeSpecified(selective
										.locations())) {
							return true;
						}
					}
				}

				return false;
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

	public void setInjectAnnotationProcessor(
			InjectAnnotationProcessor injectAnnotationProcessor) {
		this.injectAnnotationProcessor = injectAnnotationProcessor;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	/*
	 * With JUnit 4.10 is impossible to get target from a Rule, it seems that
	 * future versions will support it. For now constructor is approach is the
	 * only way.
	 */
	protected void setTarget(Object target) {
		this.target = target;
	}

}
