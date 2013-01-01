package com.lordofthejars.nosqlunit.hbase;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.hbase.model.DataSetParser;
import com.lordofthejars.nosqlunit.hbase.model.JsonDataSetParser;
import com.lordofthejars.nosqlunit.hbase.model.ParsedDataModel;

public class DefaultHBaseInsertationStrategy implements HBaseInsertationStrategy {

	@Override
	public void insert(HBaseConnectionCallback connection, InputStream dataset) throws Throwable {
		DataSetParser dataSetParser = new JsonDataSetParser();
		ParsedDataModel parsedDataset = dataSetParser.parse(dataset);
		
		DataLoader dataLoader = new DataLoader(connection.configuration());
		dataLoader.load(parsedDataset);
	}

}
