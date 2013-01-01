package com.lordofthejars.nosqlunit.hbase;

import org.apache.hadoop.conf.Configuration;

public interface HBaseConnectionCallback {

	Configuration configuration();
	
}
