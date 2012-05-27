package com.lordofthejars.nosqlunit.core;

public class OsNameSystemPropertyOperatingSystemResolver implements
		OperatingSystemResolver {

	private String osNameProperty = System.getProperty("os.name");
	
	@Override
	public OperatingSystem currentOperatingSystem() {
		return OperatingSystem.resolve(osNameProperty);
	}

}
