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

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class HBaseOperation extends AbstractCustomizableDatabaseOperation<HBaseConnectionCallback, Configuration> {

	private Configuration configuration;
	
	public HBaseOperation(HBaseConfiguration configuration) {
		this.configuration = configuration.getConfiguration();
		setInsertationStrategy(new DefaultHBaseInsertationStrategy());
		setComparisionStrategy(new DefaultHBaseComparisionStrategy());
	}
	
	@Override
	public void insert(InputStream dataScript) {
		
		insertData(dataScript);
	}

	private void insertData(InputStream dataScript) {
		try {
			executeInsertation(new HBaseConnectionCallback() {
				
				@Override
				public Configuration configuration() {
					return configuration;
				}
			}, dataScript);
		} catch (Throwable e) {
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
		return compareData(expectedData);
	}

	private boolean compareData(InputStream expectedData) throws NoSqlAssertionError {
		try {
			return executeComparision(new HBaseConnectionCallback() {
				
				@Override
				public Configuration configuration() {
					return configuration;
				}
			}, expectedData);
		} catch (NoSqlAssertionError e) {
			throw e;
		} catch (Throwable e) {
			throw new java.lang.IllegalStateException(e);
		}
	}

	@Override
	public Configuration connectionManager() {
		return configuration;
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
