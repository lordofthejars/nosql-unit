package com.lordofthejars.nosqlunit.redis.parser;

import java.io.UnsupportedEncodingException;


public class JsonToJedisConverter {

	private static final String DEFAULT_CHARSET = "UTF-8";
	
	private JsonToJedisConverter() {
		super();
	}

	public static final byte[] toByteArray(java.lang.Object object) {

		if (object instanceof Number) {
			Number number = (Number) object;
			byte[] numberByte = new byte[1];
			numberByte[0] = number.byteValue();

			return numberByte;
		} else {
			if (object instanceof Boolean) {
				Boolean bool = (Boolean) object;
				try {
					return bool.toString().getBytes(DEFAULT_CHARSET);
				} catch (UnsupportedEncodingException e) {
					throw new IllegalArgumentException(e);
				}
			} else {
				if (object instanceof String) {
					String stringValue = (String) object;
					try {
						return stringValue.getBytes(DEFAULT_CHARSET);
					} catch (UnsupportedEncodingException e) {
						throw new IllegalArgumentException(e);
					}
				} else {
					throw new IllegalArgumentException("Class type " + object.getClass()
							+ " is not supported to be converted to byte[].");
				}
			}
		}
	}

	public static final Double toDouble(java.lang.Object object) {

		if (object instanceof Number) {
			return ((Number) object).doubleValue();
		} else {
			
				throw new IllegalArgumentException("Class type " + object.getClass()
						+ " is not supported to be converted to Double.");
		}
	}
}
