package com.lordofthejars.nosqlunit.graph.parser;

import java.util.Map;

public class TypeCaster {

	private TypeCaster() {
		super();
	}

	public static String getStringType(final Object object) {
		if (object instanceof String) {
			return GraphMLTokens.STRING;
		} else if (object instanceof Integer) {
			return GraphMLTokens.INT;
		} else if (object instanceof Long) {
			return GraphMLTokens.LONG;
		} else if (object instanceof Float) {
			return GraphMLTokens.FLOAT;
		} else if (object instanceof Double) {
			return GraphMLTokens.DOUBLE;
		} else if (object instanceof Boolean) {
			return GraphMLTokens.BOOLEAN;
		} else {
			return GraphMLTokens.STRING;
		}
	}

	public static Object typeCastValue(String key, String value, Map<String, String> keyTypes) {
		String type = keyTypes.get(key);
		if (null == type || type.equals(GraphMLTokens.STRING))
			return value;
		else if (type.equals(GraphMLTokens.FLOAT))
			return Float.valueOf(value);
		else if (type.equals(GraphMLTokens.INT))
			return Integer.valueOf(value);
		else if (type.equals(GraphMLTokens.DOUBLE))
			return Double.valueOf(value);
		else if (type.equals(GraphMLTokens.BOOLEAN))
			return Boolean.valueOf(value);
		else if (type.equals(GraphMLTokens.LONG))
			return Long.valueOf(value);
		else
			return value;
	}

}
