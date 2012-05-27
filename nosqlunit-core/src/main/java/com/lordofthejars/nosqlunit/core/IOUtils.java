package com.lordofthejars.nosqlunit.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public final class IOUtils {

	private IOUtils() {
		super();
	}
	
	public static boolean deleteDir(File dir) {
	    if (dir.isDirectory()) {
	        String[] children = dir.list();
	        for (int i=0; i<children.length; i++) {
	            boolean success = deleteDir(new File(dir, children[i]));
	            if (!success) {
	                return false;
	            }
	        }
	    }

	    return dir.delete();
	}
	
	public static String readAllStreamFromClasspathBaseResource(Class<?> resourceBase, String dataLocation) throws IOException {
		return readFullStream(resourceBase.getResourceAsStream(dataLocation));
	}
	
	public static String[] readAllStreamsFromClasspathBaseResource(Class<?> resourceBase, String[] dataLocations) throws IOException {
		String[] scriptContent = new String[dataLocations.length];
		
		for (int i=0;i<dataLocations.length;i++) {
			String content = readAllStreamFromClasspathBaseResource(resourceBase, dataLocations[i]);
			scriptContent[i] = content; 
		}
		return scriptContent;
	}
	
	public static String readFullStream(InputStream data) throws IOException {
		
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(data));
		
		StringBuilder readData = new StringBuilder();
		String readLine;
		
		while((readLine = bufferedReader.readLine()) != null) {
			readData.append(readLine);
		}
		
		return readData.toString();
	}
	
}
