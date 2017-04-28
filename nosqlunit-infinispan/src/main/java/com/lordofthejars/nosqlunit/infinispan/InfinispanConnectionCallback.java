package com.lordofthejars.nosqlunit.infinispan;

import org.infinispan.commons.api.BasicCache;

public interface InfinispanConnectionCallback {

	BasicCache<Object, Object> basicCache();
	
}
