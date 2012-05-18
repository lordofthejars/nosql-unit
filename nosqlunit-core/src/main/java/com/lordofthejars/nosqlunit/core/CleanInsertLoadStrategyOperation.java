package com.lordofthejars.nosqlunit.core;

public class CleanInsertLoadStrategyOperation implements LoadStrategyOperation {

	private DatabaseOperation databaseOperation;

	public CleanInsertLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	@Override
	public void executeScripts(String[] contentDataset) {
		executeClean();
		executeInsert(contentDataset);
	}

	private void executeInsert(String[] contentDataset) {
		for (String dataScript : contentDataset) {
			this.databaseOperation.insert(dataScript);
		}
	}

	private void executeClean() {
		this.databaseOperation.deleteAll();
	}

}
