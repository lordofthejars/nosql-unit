package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteAllLoadStrategyOperation implements LoadStrategyOperation {

	private static final Logger LOGGER = LoggerFactory.getLogger(DeleteAllLoadStrategyOperation.class);
	
	private DatabaseOperation databaseOperation;

	public DeleteAllLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	@Override
	public void executeScripts(InputStream[] contentDataset) {
		
		LOGGER.debug("Calling Clean Load Strategy.");
		
		executeClean();
	}

	private void executeClean() {
		this.databaseOperation.deleteAll();
	}

}
