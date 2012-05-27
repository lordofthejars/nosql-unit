package com.lordofthejars.nosqlunit.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CommandLineExecutor {

	public Process startProcessInDirectoryAndArguments(String targetDirectory, List<String> arguments) throws IOException {
		
		ProcessBuilder processBuilder = new ProcessBuilder(arguments);
		processBuilder.directory(new File(targetDirectory));
		processBuilder.redirectErrorStream(true);
		Process pwd = processBuilder.start();
		return pwd;
	}
	
	public List<String> getConsoleOutput(Process process) throws IOException {
		
		BufferedReader outputReader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		String output;
		List<String> lines = new ArrayList<String>();
		while ((output = outputReader.readLine()) != null) {
			lines.add(output.toString());
		}
		
		return lines;
	}
	
}
