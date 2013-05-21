package com.lordofthejars.nosqlunit.core;

import java.lang.reflect.Field;


public class PropertyGetter <T> {

	public T propertyByType(Object testInstance, Class<T> type) {

		Class<?> clazz = testInstance.getClass();

		if (isTargetSet(testInstance)) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {

				if (type.isAssignableFrom(field.getType())) {
					return (T) new FieldGetter(testInstance, field).get();
				}
			}
		}

		return null;
	}

	private boolean isTargetSet(Object testInstance) {
		return testInstance != null;
	}

}
