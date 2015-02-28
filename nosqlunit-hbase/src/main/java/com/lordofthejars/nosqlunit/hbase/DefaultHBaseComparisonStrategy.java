package com.lordofthejars.nosqlunit.hbase;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class DefaultHBaseComparisonStrategy implements HBaseComparisonStrategy {

	@Override
	public boolean compare(HBaseConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError, Throwable {
		HConnection hconnection = connection(connection.configuration());
		assertData(hconnection, dataset);
		
		return true;
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
	
	private HConnection connection(Configuration configuration) {
		try {
			return HConnectionManager.createConnection(configuration);
		} catch (ZooKeeperConnectionException e) {
			throw new IllegalArgumentException(e);
		}
	}

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {
    }
	
}
