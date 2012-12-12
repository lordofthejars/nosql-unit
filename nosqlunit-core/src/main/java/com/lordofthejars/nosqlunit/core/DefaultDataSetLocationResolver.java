package com.lordofthejars.nosqlunit.core;

import static com.lordofthejars.nosqlunit.core.IOUtils.isFileAvailableOnClasspath;

import java.lang.annotation.Annotation;

import org.junit.runners.model.FrameworkMethod;

import com.lordofthejars.nosqlunit.util.DefaultClasspathLocationBuilder;


public class DefaultDataSetLocationResolver {

	private Class<?> resourceBase;
	
	public DefaultDataSetLocationResolver(Class<?> resourceBase) {
		this.resourceBase = resourceBase;
	}
	
	public Class<?> getResourceBase() {
		return resourceBase;
	}
	
	public String resolveDefaultDataSetLocation(Annotation annotation, FrameworkMethod method, String suffix) {
		
		String defaultClassAnnotatedClasspath = DefaultClasspathLocationBuilder.defaultClassAnnotatedClasspathLocation(method);
		
		if(isMethodAnnotated(method, annotation)) {
			
			String defaultMethodAnnotatedClasspathFile = DefaultClasspathLocationBuilder.defaultMethodAnnotatedClasspathLocation(
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

	private boolean isMethodAnnotated(FrameworkMethod method, Annotation annotation) {
		return method.getAnnotation(annotation.annotationType()) != null;
	}
	
}
