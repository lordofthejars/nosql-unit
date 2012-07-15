package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RefreshLoadStrategyOperation implements LoadStrategyOperation {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RefreshLoadStrategyOperation.class);

	private DatabaseOperation databaseOperation;

	public RefreshLoadStrategyOperation(DatabaseOperation databaseOperation) {
		this.databaseOperation = databaseOperation;
	}

	@Override
	public void executeScripts(InputStream[] contentDataset) {

		LOGGER.debug("Calling Refresh Load Strategy.");
		if (contentDataset.length > 0) {
			for (InputStream dataScript : contentDataset) {
				this.databaseOperation.insertNotPresent(dataScript);
			}
		} else {
			throw new IllegalArgumentException(
					"File specified in locations attribute are not present, or no files matching default name are found.");
		}
	}

}
