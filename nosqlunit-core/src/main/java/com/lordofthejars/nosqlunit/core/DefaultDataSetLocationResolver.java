package com.lordofthejars.nosqlunit.core;

import static com.lordofthejars.nosqlunit.core.IOUtils.isFileAvailableOnClasspath;

import java.lang.annotation.Annotation;

import org.junit.runners.model.FrameworkMethod;


public class DefaultDataSetLocationResolver {

	private static final String METHOD_SEPARATOR = "#";
	
	private Class<?> resourceBase;
	
	public DefaultDataSetLocationResolver(Class<?> resourceBase) {
		this.resourceBase = resourceBase;
	}
	
	public Class<?> getResourceBase() {
		return resourceBase;
	}
	
	public String resolveDefaultDataSetLocation(Annotation annotation, FrameworkMethod method, String suffix) {
		
		
		String testClassName = method.getMethod().getDeclaringClass().getName();
		String defaultClassAnnotatedClasspath = "/"
				+ testClassName.replace('.', '/');
		
		if(isMethodAnnotated(method, annotation)) {
			
			String defaultMethodAnnotatedClasspathFile = buildRequiredFilepathForMethodAnnotatation(
					method, defaultClassAnnotatedClasspath, suffix);
			
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
			FrameworkMethod method,
			String defaultClassAnnotatedClasspath, String suffix) {
		String testMethodName = method.getName();

		String defaultMethodAnnotatedClasspathFile = defaultClassAnnotatedClasspath
				+ METHOD_SEPARATOR
				+ testMethodName
				+ suffix;
		return defaultMethodAnnotatedClasspathFile;
	}

	private boolean isMethodAnnotated(FrameworkMethod method, Annotation annotation) {
		return method.getAnnotation(annotation.annotationType()) != null;
	}
	
}
