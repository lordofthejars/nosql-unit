package com.lordofthejars.nosqlunit.core;

import java.io.InputStream;


public interface LoadStrategyOperation {

	void executeScripts(InputStream[] contentDataset);

}
