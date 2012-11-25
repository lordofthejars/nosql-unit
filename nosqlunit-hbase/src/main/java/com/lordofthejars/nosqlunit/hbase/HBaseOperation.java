package com.lordofthejars.nosqlunit.hbase;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import com.lordofthejars.nosqlunit.core.DatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.hbase.model.DataSetParser;
import com.lordofthejars.nosqlunit.hbase.model.JsonDataSetParser;
import com.lordofthejars.nosqlunit.hbase.model.ParsedDataModel;

public class HBaseOperation implements DatabaseOperation<Configuration> {

	private DataLoader dataLoader;
	private Configuration configuration;
	
	public HBaseOperation(HBaseConfiguration configuration) {
		this.configuration = configuration.getConfiguration();
		this.dataLoader = new DataLoader(this.configuration);
	}
	
	@Override
	public void insert(InputStream dataScript) {
		
		DataSetParser dataSetParser = new JsonDataSetParser();
		ParsedDataModel parsedDataset = dataSetParser.parse(dataScript);
		try {
			dataLoader.load(parsedDataset);
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"Unexpected error reading data set file.", e);
		}
	}

	@Override
	public void deleteAll() {
		HConnection connection = connection();
		HBaseAdmin hBaseAdmin = hBaseAdmin(connection);
		deleteAllTables(connection, hBaseAdmin);
	}

	private void deleteAllTables(HConnection connection, HBaseAdmin hBaseAdmin) {
		try {
			HTableDescriptor[] listTables = connection.listTables();
			
			for (HTableDescriptor hTableDescriptor : listTables) {
				byte[] tableName = hTableDescriptor.getName();
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
			}
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public boolean databaseIs(InputStream expectedData) {
		HConnection connection = connection();
		assertData(connection, expectedData);
		return true;
	}

	@Override
	public Configuration connectionManager() {
		return configuration;
	}
	
	private void assertData(HConnection connection, InputStream expectedData) {
		try {
			HBaseAssertion.strictAssertEquals(connection, expectedData);
		} catch(NoSqlAssertionError e) {
			throw e;
		} catch (Throwable e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	private HBaseAdmin hBaseAdmin(HConnection connection) {
		try {
			return new HBaseAdmin(connection);
		} catch (MasterNotRunningException e) {
			throw new IllegalArgumentException(e);
		} catch (ZooKeeperConnectionException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	private HConnection connection() {
		try {
			return HConnectionManager.createConnection(this.configuration);
		} catch (ZooKeeperConnectionException e) {
			throw new IllegalArgumentException(e);
		}
	}

}
