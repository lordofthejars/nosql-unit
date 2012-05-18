package com.lordofthejars.nosqlunit.core;

public class DeleteAllLoadStrategyOperation implements LoadStrategyOperation {

	private DatabaseOperation databaseOperation;

	public DeleteAllLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	@Override
	public void executeScripts(String[] contentDataset) {
		executeClean();
	}

	private void executeClean() {
		this.databaseOperation.deleteAll();
	}

}
