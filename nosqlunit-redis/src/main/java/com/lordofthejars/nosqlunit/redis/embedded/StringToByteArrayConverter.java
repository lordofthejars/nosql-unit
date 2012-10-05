package com.lordofthejars.nosqlunit.redis.embedded;

import redis.clients.util.SafeEncoder;
import ch.lambdaj.function.convert.Converter;

public class StringToByteArrayConverter implements Converter<String, byte[]> {

	public static StringToByteArrayConverter createStringToByteArrayConverter() {
		return new StringToByteArrayConverter();
	}
	
	@Override
	public byte[] convert(String from) {
		return SafeEncoder.encode(from);
	}

}
