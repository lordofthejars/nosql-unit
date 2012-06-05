package com.lordofthejars.nosqlunit.core;

import static com.lordofthejars.nosqlunit.core.IOUtils.isFileAvailableOnClasspath;

import java.lang.annotation.Annotation;
import org.junit.runner.Description;


public class DefaultDataSetLocationResolver {

	private static final String METHOD_SEPARATOR = "#";
	
	private Class<?> resourceBase;
	
	public DefaultDataSetLocationResolver(Class<?> resourceBase) {
		this.resourceBase = resourceBase;
	}
	
	public Class<?> getResourceBase() {
		return resourceBase;
	}
	
	public String resolveDefaultDataSetLocation(Annotation annotation, Description description, String suffix) {
		
		
		String testClassName = description.getClassName();
		String defaultClassAnnotatedClasspath = "/"
				+ testClassName.replace('.', '/');
		
		if(isMethodAnnotated(description, annotation)) {
			
			String defaultMethodAnnotatedClasspathFile = buildRequiredFilepathForMethodAnnotatation(
					description, defaultClassAnnotatedClasspath, suffix);
			
			if (isFileAvailableOnClasspath(resourceBase,
					defaultMethodAnnotatedClasspathFile)) {
			
				return	defaultMethodAnnotatedClasspathFile;

			} else {
			
				String defaultClassAnnotatedClasspathFile = defaultClassAnnotatedClasspath+suffix;
				
				if (isFileAvailableOnClasspath(resourceBase,
						defaultClassAnnotatedClasspathFile)) {

					return 	defaultClassAnnotatedClasspathFile;
				
				}
				
			}
			
			
		} else {
			
			String defaultClassAnnotatedClasspathFile = defaultClassAnnotatedClasspath+suffix;
			
			if (isFileAvailableOnClasspath(resourceBase,
					defaultClassAnnotatedClasspathFile)) {

				return 	defaultClassAnnotatedClasspathFile;
			
			}
			
		}
		
		return null;
	}
	
	private String buildRequiredFilepathForMethodAnnotatation(
			Description description,
			String defaultClassAnnotatedClasspath, String suffix) {
		String testMethodName = description.getMethodName();

		String defaultMethodAnnotatedClasspathFile = defaultClassAnnotatedClasspath
				+ METHOD_SEPARATOR
				+ testMethodName
				+ suffix;
		return defaultMethodAnnotatedClasspathFile;
	}

	private boolean isMethodAnnotated(Description description, Annotation annotation) {
		return description.getAnnotation(annotation.annotationType()) != null;
	}
	
}
