package com.lordofthejars.nosqlunit.core;

import static com.lordofthejars.nosqlunit.core.IOUtils.isFileAvailableOnClasspath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;

public abstract class AbstractNoSqlTestRule implements TestRule {

	private static final String METHOD_SEPARATOR = "#";

	private Class<?> resourceBase;

	/* TODO Guice */
	private LoadStrategyFactory loadStrategyFactory = new LoadStrategyFactory();

	public AbstractNoSqlTestRule() {
	}

	public abstract DatabaseOperation getDatabaseOperation();

	public abstract String getWorkingExtension();

	@Override
	public Statement apply(final Statement base, final Description description) {
		return new Statement() {

			@Override
			public void evaluate() throws Throwable {

				resourceBase = description.getTestClass();

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

				String[] locations = shouldMatchDataSet.values();
				List<String> scriptContents = IOUtils
						.readAllStreamsFromClasspathBaseResource(resourceBase,
								locations);

				for (String jsonContent : scriptContents) {
					getDatabaseOperation().nonStrictAssertEquals(jsonContent);
				}
			}

			private void loadDataSet(UsingDataSet usingDataSet,
					Description description) throws IOException {

				String[] locations = usingDataSet.locations();

				List<String> scriptContent = new ArrayList<String>();

				if (isLocationsAttributeSpecified(locations)) {

					scriptContent.addAll(IOUtils
							.readAllStreamsFromClasspathBaseResource(
									resourceBase, locations));

				} else {

					String testClassName = description.getClassName();
					String defaultClassAnnotatedClasspath = "/"
							+ testClassName.replace('.', '/');

					if (isMethodAnnotated(description)) {

						String defaultMethodAnnotatedClasspathFile = buildRequiredFilepathForMethodAnnotatation(
								description, defaultClassAnnotatedClasspath);

						if (isFileAvailableOnClasspath(resourceBase,
								defaultMethodAnnotatedClasspathFile)) {

							scriptContent
									.add(IOUtils
											.readAllStreamFromClasspathBaseResource(
													resourceBase,
													defaultMethodAnnotatedClasspathFile));

						} else {

							String defaultClassAnnotatedClasspathFile = defaultClassAnnotatedClasspath+"."+getWorkingExtension();
							
							if (isFileAvailableOnClasspath(resourceBase,
									defaultClassAnnotatedClasspathFile)) {

								scriptContent
										.add(IOUtils
												.readAllStreamFromClasspathBaseResource(
														resourceBase,
														defaultClassAnnotatedClasspathFile));
							}

						}

					} else {

						String defaultClassAnnotatedClasspathFile = defaultClassAnnotatedClasspath+"."+getWorkingExtension();
						
						if (isFileAvailableOnClasspath(resourceBase,
								defaultClassAnnotatedClasspathFile)) {

							scriptContent
									.add(IOUtils
											.readAllStreamFromClasspathBaseResource(
													resourceBase,
													defaultClassAnnotatedClasspathFile));
						}
						
					}
				}

				if (scriptContent.size() > 0) {

					LoadStrategyEnum loadStrategyEnum = usingDataSet
							.loadStrategy();
					LoadStrategyOperation loadStrategyOperation = loadStrategyFactory
							.getLoadStrategyInstance(loadStrategyEnum,
									getDatabaseOperation());
					loadStrategyOperation.executeScripts(scriptContent
							.toArray(new String[scriptContent.size()]));

				} else {
					throw new IllegalArgumentException("File specified in locations attribute are not present, or no files matching [] or [] are found.");
				}

			}

			private String buildRequiredFilepathForMethodAnnotatation(
					Description description,
					String defaultClassAnnotatedClasspath) {
				String testMethodName = description.getMethodName();

				String defaultMethodAnnotatedClasspathFile = defaultClassAnnotatedClasspath
						+ METHOD_SEPARATOR
						+ testMethodName
						+ "."
						+ getWorkingExtension();
				return defaultMethodAnnotatedClasspathFile;
			}

			private boolean isMethodAnnotated(Description description) {
				return description.getAnnotation(UsingDataSet.class) != null;
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
