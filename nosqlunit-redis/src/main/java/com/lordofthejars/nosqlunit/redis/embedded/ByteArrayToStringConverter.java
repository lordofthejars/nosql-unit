package com.lordofthejars.nosqlunit.redis.embedded;

import redis.clients.util.SafeEncoder;
import ch.lambdaj.function.convert.Converter;

public class ByteArrayToStringConverter implements Converter<byte[], String> {

	public static ByteArrayToStringConverter createByteArrayToStringConverter() {
		return new ByteArrayToStringConverter();
	}
	
	@Override
	public String convert(byte[] from) {
		if(from != null) {
			return SafeEncoder.encode(from);
		} else {
			return null;
		}
	}

}
