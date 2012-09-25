package com.lordofthejars.nosqlunit.redis.embedded;

import static java.nio.ByteBuffer.wrap;
import java.nio.ByteBuffer;

import ch.lambdaj.function.convert.Converter;

public class ByteArray2ByteBufferConverter implements Converter<byte[], ByteBuffer> {

	@Override
	public ByteBuffer convert(byte[] from) {
		return wrap(from);
	}

}
