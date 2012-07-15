package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertLoadStrategyOperation implements LoadStrategyOperation {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(InsertLoadStrategyOperation.class);

	private DatabaseOperation databaseOperation;

	public InsertLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	@Override
	public void executeScripts(InputStream[] contentDataset) {

		LOGGER.debug("Calling Insert Load Strategy.");
		if (contentDataset.length > 0) {
			executeInsert(contentDataset);
		} else {
			throw new IllegalArgumentException(
					"File specified in locations attribute are not present, or no files matching default name are found.");
		}
	}

	private void executeInsert(InputStream[] contentDataset) {
		for (InputStream dataScript : contentDataset) {
			this.databaseOperation.insert(dataScript);
		}
	}

}
