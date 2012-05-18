package com.lordofthejars.nosqlunit.core;

public class InsertLoadStrategyOperation implements LoadStrategyOperation {

	private DatabaseOperation databaseOperation;
	
	public InsertLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	
	@Override
	public void executeScripts(String[] contentDataset) {
		executeInsert(contentDataset);
	}

	private void executeInsert(String[] contentDataset) {
		for (String dataScript : contentDataset) {
			this.databaseOperation.insert(dataScript);
		}
	}
	
}
