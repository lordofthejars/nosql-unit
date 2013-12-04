package com.lordofthejars.nosqlunit.hbase;

import static ch.lambdaj.Lambda.selectFirst;
import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;

import ch.lambdaj.function.convert.Converter;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import com.lordofthejars.nosqlunit.hbase.model.DataSetParser;
import com.lordofthejars.nosqlunit.hbase.model.JsonDataSetParser;
import com.lordofthejars.nosqlunit.hbase.model.ParsedColumnFamilyModel;
import com.lordofthejars.nosqlunit.hbase.model.ParsedColumnModel;
import com.lordofthejars.nosqlunit.hbase.model.ParsedDataModel;
import com.lordofthejars.nosqlunit.hbase.model.ParsedRowModel;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseAssertion {

	public static void strictAssertEquals(HConnection connection, InputStream dataset) throws Throwable {

		DataSetParser dataSetParser = new JsonDataSetParser();
		ParsedDataModel parsedDataset = dataSetParser.parse(dataset);

		Configuration configuration = connection.getConfiguration();

		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);

		byte[] expectedTableName = checkTableName(parsedDataset, hBaseAdmin);

		HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor(expectedTableName);

		List<ParsedColumnFamilyModel> expectedColumnFamilies = parsedDataset.getColumnFamilies();
		checkNumberOfColumnFamilies(tableDescriptor, expectedColumnFamilies);
		checkColumnFamilies(connection, configuration, expectedTableName, tableDescriptor, expectedColumnFamilies);

	}

	private static void checkColumnFamilies(HConnection connection, Configuration configuration,
			byte[] expectedTableName, HTableDescriptor tableDescriptor,
			List<ParsedColumnFamilyModel> expectedColumnFamilies) throws Error, Throwable, IOException {
		
		for (ParsedColumnFamilyModel parsedColumnFamilyModel : expectedColumnFamilies) {
			checkColumnFamily(connection, configuration, expectedTableName, tableDescriptor, expectedColumnFamilies,
					parsedColumnFamilyModel);
		}
	}

	private static void checkColumnFamily(HConnection connection, Configuration configuration,
			byte[] expectedTableName, HTableDescriptor tableDescriptor,
			List<ParsedColumnFamilyModel> expectedColumnFamilies, ParsedColumnFamilyModel parsedColumnFamilyModel)
			throws Error, Throwable, IOException {
		
		byte[] expectedFamilyName = checkColumnFamilyName(tableDescriptor, parsedColumnFamilyModel);

		List<ParsedRowModel> expectedRows = checkNumberOfRows(connection, expectedTableName,
				parsedColumnFamilyModel, expectedFamilyName);
		checkRows(configuration, expectedTableName, expectedColumnFamilies, parsedColumnFamilyModel, expectedRows);
	}

	private static void checkRows(Configuration configuration, byte[] expectedTableName,
			List<ParsedColumnFamilyModel> expectedColumnFamilies, ParsedColumnFamilyModel parsedColumnFamilyModel,
			List<ParsedRowModel> expectedRows) throws IOException, Error {
		
		for (ParsedRowModel parsedRowModel : expectedRows) {
			checkRow(configuration, expectedTableName, expectedColumnFamilies, parsedColumnFamilyModel, parsedRowModel);
		}
	}

	private static void checkRow(Configuration configuration, byte[] expectedTableName,
			List<ParsedColumnFamilyModel> expectedColumnFamilies, ParsedColumnFamilyModel parsedColumnFamilyModel,
			ParsedRowModel parsedRowModel) throws IOException, Error {
		
		HTable table = new HTable(configuration, expectedTableName);
		Get get = new Get(parsedRowModel.getKeyInBytes());
		get.addFamily(Bytes.toBytes(parsedColumnFamilyModel.getName()));
		Result result = table.get(get);
		
		checkRowName(parsedRowModel, result);
		
		KeyValue[] raws = result.raw();

		checkNumberOfColumns(expectedColumnFamilies, parsedColumnFamilyModel, raws, parsedRowModel);
		checkRowValues(raws, parsedRowModel);
	}

	private static void checkRowName(ParsedRowModel parsedRowModel, Result result) throws Error {
		if(result.isEmpty()) {
			throw FailureHandler.createFailure("Expected row name is %s but is not found.",
					parsedRowModel.getKey());
		}
	}

	private static void checkRowValues(KeyValue[] raws, ParsedRowModel parsedRowModel) throws Error {
		
		List<ParsedColumnModel> expectedColumns = parsedRowModel.getColumns();
		for (KeyValue raw : raws) {

			final byte[] qualifier = raw.getQualifier();
			final byte[] value = raw.getValue();
			boolean found=false;

			for (ParsedColumnModel expectedColumn : expectedColumns) {
				if(Bytes.equals(Bytes.toBytes(expectedColumn.getName()), qualifier)){
					if(Bytes.equals(expectedColumn.getValueInBytes(), value)) {
						found=true;
					}
					break;
				}
			}
			
			if(!found){
				throw FailureHandler.createFailure("Expected column are not found. Encountered column with name %s and value %s is not found.",
					getName(raws), getValue(raws));
			}
		}
		
	}

	private static void checkNumberOfColumns(List<ParsedColumnFamilyModel> expectedColumnFamilies,
			ParsedColumnFamilyModel parsedColumnFamilyModel, KeyValue[] raws, ParsedRowModel parsedRowModel)
			throws Error {
		
		List<ParsedColumnModel> expectedColumns = parsedRowModel.getColumns();
		
		int numberOfColumns = raws.length;
		int expectedNumberOfColumns = expectedColumns.size();

		if (numberOfColumns != expectedNumberOfColumns) {
			throw FailureHandler.createFailure("Expected number of columns for %s are %s but %s are found.",
					parsedRowModel.getKey(), expectedNumberOfColumns, numberOfColumns);
		}
	}

	private static List<ParsedRowModel> checkNumberOfRows(HConnection connection, byte[] expectedTableName,
			ParsedColumnFamilyModel parsedColumnFamilyModel, byte[] expectedFamilyName) throws Throwable, Error {
		List<ParsedRowModel> expectedRows = parsedColumnFamilyModel.getRows();

		long numberOfRows = countNumberOfRows(connection, expectedTableName, expectedFamilyName);
		int expectedNumberOfRows = expectedRows.size();

		if (numberOfRows != expectedNumberOfRows) {
			throw FailureHandler.createFailure("Expected number of rows are %s but %s are found.",
					expectedNumberOfRows, numberOfRows);
		}
		return expectedRows;
	}

	private static byte[] checkColumnFamilyName(HTableDescriptor tableDescriptor,
			ParsedColumnFamilyModel parsedColumnFamilyModel) throws Error {
		byte[] expectedFamilyName = parsedColumnFamilyModel.getName().getBytes();

		if (!tableDescriptor.hasFamily(expectedFamilyName)) {
			throw FailureHandler.createFailure("Expected name of column family is %s but was not found.",
					parsedColumnFamilyModel.getName());
		}
		return expectedFamilyName;
	}

	private static void checkNumberOfColumnFamilies(HTableDescriptor tableDescriptor,
			List<ParsedColumnFamilyModel> expectedColumnFamilies) throws Error {
		Set<byte[]> currentFamiliesKeys = tableDescriptor.getFamiliesKeys();

		int expectedNumberOfColumnFamilies = expectedColumnFamilies.size();
		int currentNumberOfColumnFamilies = currentFamiliesKeys.size();

		if (expectedNumberOfColumnFamilies != currentNumberOfColumnFamilies) {
			throw FailureHandler.createFailure("Expected number of column families are %s but was %s.",
					expectedNumberOfColumnFamilies, currentNumberOfColumnFamilies);
		}
	}

	private static byte[] checkTableName(ParsedDataModel parsedDataset, HBaseAdmin hBaseAdmin) throws IOException,
			Error {
		byte[] expectedTableName = parsedDataset.getName().getBytes();

		if (!hBaseAdmin.tableExists(expectedTableName)) {
			throw FailureHandler.createFailure("Table %s is not found.", parsedDataset.getName());
		}
		return expectedTableName;
	}

	private static String getValue(KeyValue[] raws) {
		return toStringValue().convert(raws[0].getValue());
	}

	private static String getName(KeyValue[] raws) {
		return toStringValue().convert(raws[0].getQualifier());
	}

	private static Converter<byte[], String> toStringValue() {
		return new Converter<byte[], String>() {

			@Override
			public String convert(byte[] from) {
				try {
					return new String(from, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new IllegalArgumentException(e);
				}
			}
		};
	}

	private static long countNumberOfRows(HConnection connection, byte[] tableName, byte[] columnFamily)
			throws Throwable {
		AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
		Scan scan = new Scan();
		scan.addFamily(columnFamily);
		return aggregationClient.rowCount(tableName, null, scan);
	}

}
