package com.lordofthejars.nosqlunit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanInsertLoadStrategyOperation implements LoadStrategyOperation {

	private static final Logger LOGGER = LoggerFactory.getLogger(CleanInsertLoadStrategyOperation.class);
	
	private DatabaseOperation databaseOperation;

	public CleanInsertLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	@Override
	public void executeScripts(String[] contentDataset) {
		
		LOGGER.debug("Calling Clean and Insert Load Strategy.");
		
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
