package com.lordofthejars.nosqlunit.redis.embedded;

import java.io.UnsupportedEncodingException;

import redis.clients.util.SafeEncoder;

public class ByteArrayIncrement {

	private ByteArrayIncrement() {
		super();
	}
	
	public static long incrementValue(final long value, byte[] elementToUpdate) throws UnsupportedEncodingException, NumberFormatException {
		String elementString = SafeEncoder.encode(elementToUpdate);
		long longValue = Long.parseLong(elementString);
		longValue += value;
		return longValue;
	}
	
}
