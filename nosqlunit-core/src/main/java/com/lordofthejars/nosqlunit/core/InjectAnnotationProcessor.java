package com.lordofthejars.nosqlunit.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Qualifier;

public class InjectAnnotationProcessor {

	private String identifier;

	public InjectAnnotationProcessor(String identifier) {
		this.identifier = identifier;
	}

	void processInjectAnnotation(Class<?> clazz, Object testInstance,
			Object injectionObject) {

		if (isTargetSet(testInstance)) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				Annotation injectAnnotation = field.getAnnotation(Inject.class);
				if (injectAnnotation != null) {
					Annotation namedAnnotation = field
							.getAnnotation(Named.class);

					if (isNamedAnnotationNotPresent(namedAnnotation)
							|| isIdentifierValueInNamedAnnotation(namedAnnotation)) {

						if (field.getType().isInstance(injectionObject)) {
							new FieldSetter(testInstance, field)
									.set(injectionObject);
						}
					}
				}
			}
		}
	}

	private boolean isIdentifierValueInNamedAnnotation(
			Annotation namedAnnotation) {
		String namedValue = ((Named) namedAnnotation).value();
		return "".equals(namedValue) || namedValue.equals(
				identifier);
	}

	private boolean isNamedAnnotationNotPresent(Annotation namedAnnotation) {
		return namedAnnotation == null;
	}

	private boolean isTargetSet(Object testInstance) {
		return testInstance != null;
	}
}
