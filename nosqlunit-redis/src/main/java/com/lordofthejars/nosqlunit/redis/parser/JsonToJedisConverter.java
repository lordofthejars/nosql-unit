package com.lordofthejars.nosqlunit.redis.parser;

public class JsonToJedisConverter {

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
				return bool.toString().getBytes();
			} else {
				if (object instanceof String) {
					String stringValue = (String) object;
					return stringValue.getBytes();
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
