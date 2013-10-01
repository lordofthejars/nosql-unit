package com.lordofthejars.nosqlunit.core;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;


public class PropertyGetter <T> {

	public T propertyByType(Object testInstance, Class<T> type) {

		Class<?> clazz = testInstance.getClass();

		if (isTargetSet(testInstance)) {
            List<Field> fields = getAllFields(new LinkedList<Field>(), clazz);
			for (Field field : fields) {

				if (type.isAssignableFrom(field.getType())) {
					return (T) new FieldGetter(testInstance, field).get();
				}
			}
		}

		return null;
	}

    public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        for (Field field: type.getDeclaredFields()) {
            fields.add(field);
        }

        if (type.getSuperclass() != null) {
            fields = getAllFields(fields, type.getSuperclass());
        }

        return fields;
    }

	private boolean isTargetSet(Object testInstance) {
		return testInstance != null;
	}

}
