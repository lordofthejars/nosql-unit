package com.lordofthejars.nosqlunit.graph.parser;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.primitives.Primitives;

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
		} else if (isArray(object)) {
		    return object.getClass().getSimpleName();
		} else {
			return GraphMLTokens.STRING;
		}
	}

	public static boolean isArray(Object obj) {
	    return obj!=null && obj.getClass().isArray();
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
		else if (type.contains("[]"))
            return castToArray(type, value);
		else
			return value;
	}

	/**
     * Creates a primitive array of the specified type (eg. long[] or int[])
     * Using 'value' as a comma-delimited String.
     */
    static Object castToArray(final String type, final String value) {
        try {
            final String[] values = value.split(",");
            final String className = "java.lang." + StringUtils.capitalize(type.replace("[]", ""));
            final Class<?> klass = Class.forName(className);
            final Constructor<?> konstructor = klass.getDeclaredConstructor(String.class);
            final Object array = Array.newInstance(Primitives.unwrap(klass), values.length);
            for (int i = 0; i < values.length; i++) {
                Array.set(array, i, konstructor.newInstance(values[i].trim()));
            }
            return array;
        } catch (final Exception e) {
            throw new IllegalStateException("Could not cast " + value + " to " + type, e);
        }
    }
	
}
