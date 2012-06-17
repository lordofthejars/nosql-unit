package com.lordofthejars.nosqlunit.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

import javax.inject.Inject;

public class InjectAnnotationProcessor {

	void processInjectAnnotation(Class<?> clazz, Object testInstance,
			Object injectionObject) {

		if (isTargetSet(testInstance)) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				for (Annotation annotation : field.getAnnotations()) {
					if (annotation instanceof Inject) {
						if (field.getType().isInstance(
								injectionObject)) {
							new FieldSetter(testInstance, field)
									.set(injectionObject);
						}
					}
				}
			}
		}
	}

	private boolean isTargetSet(Object testInstance) {
		return testInstance != null;
	}
}
