package com.lordofthejars.nosqlunit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RefreshLoadStrategyOperation implements LoadStrategyOperation {

	private static final Logger LOGGER = LoggerFactory.getLogger(RefreshLoadStrategyOperation.class);
	
	private DatabaseOperation databaseOperation;

	public RefreshLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}
	
	@Override
	public void executeScripts(String[] contentDataset) {
		
		LOGGER.debug("Calling Refresh Load Strategy.");
		
		for (String dataScript : contentDataset) {
			this.databaseOperation.insertNotPresent(dataScript);
		}
	}

}
