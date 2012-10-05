package com.lordofthejars.nosqlunit.redis.embedded;

import redis.clients.util.SafeEncoder;
import ch.lambdaj.function.convert.Converter;

public class DoubleToStringByteArrayConverter implements Converter<Double, byte[]> {

	public static DoubleToStringByteArrayConverter createDoubleToStringByteArrayConverter() {
		return new DoubleToStringByteArrayConverter();
	}
	
	@Override
	public byte[] convert(Double from) {
		String doubleValue = Double.toString(from);
		return SafeEncoder.encode(doubleValue);
	}

}
