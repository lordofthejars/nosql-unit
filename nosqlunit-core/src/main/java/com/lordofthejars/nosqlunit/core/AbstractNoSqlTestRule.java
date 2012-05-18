package com.lordofthejars.nosqlunit.core;

import java.io.IOException;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.lordofthejars.nosqlunit.annotation.DataSet;
import com.lordofthejars.nosqlunit.annotation.ExpectedDataSet;

public abstract class AbstractNoSqlTestRule implements TestRule {

	private Class<?> resourceBase;
	
	public AbstractNoSqlTestRule(Class<?> resourceBase) {
			this.resourceBase = resourceBase;
	}
	
	protected abstract DatabaseOperation getDatabaseOperation();
	
	@Override
	public Statement apply(final Statement base, final Description description) {
		return new Statement() {
			
			@Override
			public void evaluate() throws Throwable {
				

				DataSet dataSet = description.getAnnotation(DataSet.class);
				
				if(isTestMethodAnnotatedWithDataSet(dataSet)) {
					loadDataSet(dataSet);
				}
				
				base.evaluate();
				
				ExpectedDataSet expectedDataSet = description.getAnnotation(ExpectedDataSet.class);
				
				if(isTestMethodAnnotatedWithExpectedDataSet(expectedDataSet)) {
					assertExpectation(expectedDataSet);
				}
				
			}

			private void assertExpectation(ExpectedDataSet expectedDataSet)
					throws IOException {
				String[] locations = expectedDataSet.values();
				String[] scriptContents = IOUtils.readAllStreamsFromClasspathBaseResource(resourceBase, locations);
				
				for (String jsonContent : scriptContents) {
					getDatabaseOperation().nonStrictAssertEquals(jsonContent);
				}
			}

			private void loadDataSet(DataSet dataSet) throws IOException {
				String[] locations = dataSet.locations();
				String[] scriptContent = IOUtils.readAllStreamsFromClasspathBaseResource(resourceBase, locations);
				
				LoadStrategyEnum loadStrategyEnum = dataSet.loadStrategy();
				LoadStrategyOperation loadStrategyOperation = LoadStrategyFactory.getLoadStrategyInstance(loadStrategyEnum, getDatabaseOperation());
				loadStrategyOperation.executeScripts(scriptContent);
			}

			private boolean isTestMethodAnnotatedWithExpectedDataSet(
					ExpectedDataSet expectedDataSet) {
				return expectedDataSet != null;
			}

			private boolean isTestMethodAnnotatedWithDataSet(DataSet dataSet) {
				return dataSet != null;
			}
		};
	}
	
}
