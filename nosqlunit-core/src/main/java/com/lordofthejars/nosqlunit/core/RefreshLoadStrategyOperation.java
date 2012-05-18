package com.lordofthejars.nosqlunit.core;

public class RefreshLoadStrategyOperation implements LoadStrategyOperation {

	private DatabaseOperation databaseOperation;

	public RefreshLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}
	
	@Override
	public void executeScripts(String[] contentDataset) {
		for (String dataScript : contentDataset) {
			this.databaseOperation.insertNotPresent(dataScript);
		}
	}

}
