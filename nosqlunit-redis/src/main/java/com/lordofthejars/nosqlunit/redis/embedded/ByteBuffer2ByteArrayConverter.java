package com.lordofthejars.nosqlunit.redis.embedded;

import java.nio.ByteBuffer;

import ch.lambdaj.function.convert.Converter;

public class ByteBuffer2ByteArrayConverter implements Converter<ByteBuffer, byte[]> {

	public static ByteBuffer2ByteArrayConverter createByteBufferConverter() {
		return new ByteBuffer2ByteArrayConverter();
	}

	@Override
	public byte[] convert(ByteBuffer from) {
		return from.array();
	}

}
