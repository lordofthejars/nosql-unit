package com.lordofthejars.nosqlunit.util;

import org.junit.runners.model.FrameworkMethod;

public class DefaultClasspathLocationBuilder {

	private static final String METHOD_SEPARATOR = "#";
	
	public static final String defaultClassAnnotatedClasspathLocation(FrameworkMethod method) {
		String testClassName = method.getMethod().getDeclaringClass().getName();
		String defaultClassAnnotatedClasspath = "/"
				+ testClassName.replace('.', '/');
		
		return defaultClassAnnotatedClasspath;
	}
	
	public static String defaultMethodAnnotatedClasspathLocation(
			FrameworkMethod method,
			String defaultClassAnnotatedClasspath, String suffix) {
		String testMethodName = method.getName();

		String defaultMethodAnnotatedClasspathFile = defaultClassAnnotatedClasspath
				+ METHOD_SEPARATOR
				+ testMethodName
				+ suffix;
		return defaultMethodAnnotatedClasspathFile;
	}
	
}
