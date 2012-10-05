package com.lordofthejars.nosqlunit.redis.embedded;

import java.nio.ByteBuffer;

import redis.clients.util.SafeEncoder;
import ch.lambdaj.function.convert.Converter;

public class ByteBufferAsString2DoubleConverter implements Converter<ByteBuffer, Double> {

	public static ByteBufferAsString2DoubleConverter createByteBufferAsStringToDoubleConverter() {
		return new ByteBufferAsString2DoubleConverter();
	}
	
	@Override
	public Double convert(ByteBuffer from) {
		String value = SafeEncoder.encode(from.array());
		return Double.valueOf(value);
	}

}
