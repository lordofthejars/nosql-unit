package com.lordofthejars.nosqlunit.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseUtils {

	
	public boolean isConnectionPossible(Configuration config) {
		 try {
				HBaseAdmin.checkHBaseAvailable(config);
				return true;
			} catch (MasterNotRunningException e) {
				return false;
			} catch (ZooKeeperConnectionException e) {
				return false;
			}
	}
	
}
