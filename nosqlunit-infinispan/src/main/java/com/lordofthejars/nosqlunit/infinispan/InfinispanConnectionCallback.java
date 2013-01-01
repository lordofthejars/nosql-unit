package com.lordofthejars.nosqlunit.infinispan;

import org.infinispan.api.BasicCache;

public interface InfinispanConnectionCallback {

	BasicCache<Object, Object> basicCache();
	
}
