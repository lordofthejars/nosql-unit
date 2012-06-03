package com.lordofthejars.nosqlunit.core;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

public class WhenClasspathResourceIsSet {

	@Test
	public void content_as_string_should_be_loaded() throws IOException {
		
		String[] locations = new String[]{"classpathContent.txt"};
		
		List<String> content = IOUtils.readAllStreamsFromClasspathBaseResource(WhenClasspathResourceIsSet.class, locations);
		
		assertThat(content.get(0), is("Hello My Name is Jimmy Pop"));
		
	}
	
}
