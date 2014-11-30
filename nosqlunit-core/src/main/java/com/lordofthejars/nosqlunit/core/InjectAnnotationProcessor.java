package com.lordofthejars.nosqlunit.core;

import java.security.PrivilegedAction;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
		    
		    Set<Field> fields = new HashSet<Field>();
		    fields.addAll(getFieldsWithAnnotation(clazz, Inject.class));
		    fields.addAll(getFieldsWithAnnotation(clazz, ConnectionManager.class));
		    fields.addAll(getFieldsWithAnnotation(clazz, ByContainer.class));
		    
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

    public static List<Field> getFieldsWithAnnotation(final Class<?> source,
            final Class<? extends Annotation> annotationClass) {
        List<Field> declaredAccessableFields = AccessController
                .doPrivileged(new PrivilegedAction<List<Field>>() {
                    public List<Field> run() {
                        List<Field> foundFields = new ArrayList<Field>();
                        Class<?> nextSource = source;
                        while (nextSource != Object.class) {
                            for (Field field : nextSource.getDeclaredFields()) {
                                if (field.isAnnotationPresent(annotationClass)) {
                                    if (!field.isAccessible()) {
                                        field.setAccessible(true);
                                    }
                                    foundFields.add(field);
                                }
                            }
                            nextSource = nextSource.getSuperclass();
                        }
                        return foundFields;
                    }
                });
        return declaredAccessableFields;
    }

    private boolean isInjectedAndNotExternallyManaged(
            Annotation injectAnnotation, Annotation byContainerAnnotation,
            Annotation connectionManagerAnnotation) {
        return (injectAnnotation != null && byContainerAnnotation == null)
                || connectionManagerAnnotation != null;
    }

    private boolean isIdentifierValueInNamedAnnotation(
            Annotation namedAnnotation) {
        String namedValue = ((Named) namedAnnotation).value();
        return "".equals(namedValue) || namedValue.equals(identifier);
    }

    private boolean isNamedAnnotationNotPresent(Annotation namedAnnotation) {
        return namedAnnotation == null;
    }

    private boolean isTargetSet(Object testInstance) {
        return testInstance != null;
    }
}
