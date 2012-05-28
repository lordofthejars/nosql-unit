package com.lordofthejars.nosqlunit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertLoadStrategyOperation implements LoadStrategyOperation {

	private static final Logger LOGGER = LoggerFactory.getLogger(InsertLoadStrategyOperation.class);
	
	private DatabaseOperation databaseOperation;
	
	public InsertLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	
	@Override
	public void executeScripts(String[] contentDataset) {
		
		LOGGER.debug("Calling Insert Load Strategy.");
		
		executeInsert(contentDataset);
	}

	private void executeInsert(String[] contentDataset) {
		for (String dataScript : contentDataset) {
			this.databaseOperation.insert(dataScript);
		}
	}
	
}
