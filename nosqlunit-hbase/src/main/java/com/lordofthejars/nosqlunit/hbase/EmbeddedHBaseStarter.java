package com.lordofthejars.nosqlunit.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class EmbeddedHBaseStarter {

	private HBaseTestingUtility testUtil;
	
	public HBaseTestingUtility startSingleMiniCluster(Configuration configuration) throws Exception {
		testUtil = new HBaseTestingUtility(configuration);
		testUtil.startMiniCluster(1);
		
		return testUtil;
	}
	
	public void stopMiniCluster() {
		if(testUtil != null) {
			try {
				testUtil.shutdownMiniCluster();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}
	
}
