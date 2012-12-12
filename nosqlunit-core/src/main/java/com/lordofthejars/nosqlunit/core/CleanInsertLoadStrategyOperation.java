package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanInsertLoadStrategyOperation implements LoadStrategyOperation {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CleanInsertLoadStrategyOperation.class);

	private DatabaseOperation databaseOperation;

	public CleanInsertLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	@Override
	public void executeScripts(InputStream[] contentDataset) {

		LOGGER.debug("Calling Clean and Insert Load Strategy.");

		if (contentDataset.length > 0) {
			executeClean();
			executeInsert(contentDataset);
		} else {
			throw new IllegalArgumentException(
					"File specified in locations property are not present, or no files matching default name are found.");
		}
	}

	private void executeInsert(InputStream[] contentDataset) {
		for (InputStream dataScript : contentDataset) {
			this.databaseOperation.insert(dataScript);
		}
	}

	private void executeClean() {
		this.databaseOperation.deleteAll();
	}

}
