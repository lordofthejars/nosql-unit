package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.lordofthejars.nosqlunit.redis.embedded.ExpirationDatatypeOperations.TtlState;

public interface RedisDatatypeOperations {

	long getNumberOfKeys();
	void flushAllKeys();
	Long del(byte[]... keys);
	boolean exists(byte[] key);
	boolean renameKey(byte[] key, byte[] newKey);
	void addExpirationTime(byte[] key, long duration, TimeUnit unit);
	void addExpirationAt(byte[] key, long time, TimeUnit unit);
	TtlState timedoutState(byte[] key);
	boolean removeExpiration(byte[] key);
	long remainingTime(byte[] key);
	List<byte[]> keys();
	String type();
	List<byte[]> sort(byte[] key);
}
