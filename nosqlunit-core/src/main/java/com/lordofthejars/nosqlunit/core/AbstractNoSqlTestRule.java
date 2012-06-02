package com.lordofthejars.nosqlunit.core;

import java.io.IOException;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;

public abstract class AbstractNoSqlTestRule implements TestRule {

	private Class<?> resourceBase;
	
	/*TODO Guice*/
	private LoadStrategyFactory loadStrategyFactory = new LoadStrategyFactory();
	
	public AbstractNoSqlTestRule(Class<?> resourceBase) {
			this.resourceBase = resourceBase;
	}
	
	public abstract DatabaseOperation getDatabaseOperation();
	
	@Override
	public Statement apply(final Statement base, final Description description) {
		return new Statement() {
			
			@Override
			public void evaluate() throws Throwable {

				UsingDataSet usingDataSet = getUsingDataSetAnnotation();
				
				if(isTestAnnotatedWithDataSet(usingDataSet)) {
					loadDataSet(usingDataSet);
				}
				
				base.evaluate();
				
				ShouldMatchDataSet shouldMatchDataSet = getShouldMatchDataSetAnnotation();
				
				if(isTestAnnotatedWithExpectedDataSet(shouldMatchDataSet)) {
					assertExpectation(shouldMatchDataSet);
				}
				
			}

			private ShouldMatchDataSet getShouldMatchDataSetAnnotation() {
				
				ShouldMatchDataSet shouldMatchDataSet = description.getAnnotation(ShouldMatchDataSet.class);
				
				if(!isTestAnnotatedWithExpectedDataSet(shouldMatchDataSet)) {
					
					Class<?> testClass = description.getTestClass();
					shouldMatchDataSet = testClass.getAnnotation(ShouldMatchDataSet.class);
					
				}
				
				return shouldMatchDataSet;
			}
			
			private UsingDataSet getUsingDataSetAnnotation() {
				
				
				UsingDataSet usingDataSet = description.getAnnotation(UsingDataSet.class);
				
				if(!isTestAnnotatedWithDataSet(usingDataSet)) {

					Class<?> testClass = description.getTestClass();
					usingDataSet = testClass.getAnnotation(UsingDataSet.class);
				
				}
				
				return usingDataSet;
			}
			
			private void assertExpectation(ShouldMatchDataSet shouldMatchDataSet)
					throws IOException {
				String[] locations = shouldMatchDataSet.values();
				String[] scriptContents = IOUtils.readAllStreamsFromClasspathBaseResource(resourceBase, locations);
				
				for (String jsonContent : scriptContents) {
					getDatabaseOperation().nonStrictAssertEquals(jsonContent);
				}
			}

			private void loadDataSet(UsingDataSet usingDataSet) throws IOException {
				String[] locations = usingDataSet.locations();
				String[] scriptContent = IOUtils.readAllStreamsFromClasspathBaseResource(resourceBase, locations);
				
				LoadStrategyEnum loadStrategyEnum = usingDataSet.loadStrategy();
				LoadStrategyOperation loadStrategyOperation = loadStrategyFactory.getLoadStrategyInstance(loadStrategyEnum, getDatabaseOperation());
				loadStrategyOperation.executeScripts(scriptContent);
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
	
	public void setResourceBase(Class<?> resourceBase) {
		this.resourceBase = resourceBase;
	}
	
}
