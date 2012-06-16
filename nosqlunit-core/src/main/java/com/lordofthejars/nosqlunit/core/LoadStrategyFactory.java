package com.lordofthejars.nosqlunit.core;

public interface LoadStrategyFactory {

	LoadStrategyOperation getLoadStrategyInstance(
			LoadStrategyEnum loadStrategyEnum,
			DatabaseOperation databaseOperation);

}