package com.lordofthejars.nosqlunit.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

import javax.inject.Inject;
import javax.inject.Named;

import com.lordofthejars.nosqlunit.annotation.ByContainer;
import com.lordofthejars.nosqlunit.annotation.ConnectionManager;

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
				Annotation connectionManagerAnnotation = field.getAnnotation(ConnectionManager.class);
				Annotation byContainerAnnotation = field.getAnnotation(ByContainer.class);
				
				if (isInjectedAndNotExternallyManaged(injectAnnotation, byContainerAnnotation, connectionManagerAnnotation)) {
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

	private boolean isInjectedAndNotExternallyManaged(Annotation injectAnnotation, Annotation byContainerAnnotation, Annotation connectionManagerAnnotation) {
		return (injectAnnotation != null && byContainerAnnotation == null) || connectionManagerAnnotation != null;
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
