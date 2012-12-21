package com.lordofthejars.nosqlunit.core;

import java.lang.reflect.Field;

public class FieldGetter {

	private final Object target;
	private final Field field;

	public FieldGetter(Object target, Field field) {
		this.target = target;
		this.field = field;
	}

	public Object get() {
		AccessibilityChanger changer = new AccessibilityChanger();
		changer.enableAccess(field);

		try {
			return field.get(target);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Access not authorized on field '" + field + "' of object '" + target, e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Wrong argument on field '" + field + "' of object '" + target + e.getMessage(),
					e);
		} finally {
			changer.safelyDisableAccess(field);
		}
	}

}
