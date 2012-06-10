package com.lordofthejars.nosqlunit.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class WhenAConnectionIsManaged {

	@Test
	public void connection_should_be_registered_at_start() {
		
		ConnectionManagement connectionManagement = ConnectionManagement.getInstance();
		connectionManagement.addConnection("localhost", 111);
		
		assertThat(connectionManagement.isConnectionRegistered("localhost", 111), is(true));
		
		connectionManagement.removeConnection("localhost", 111);
		
	}

	@Test
	public void connection_should_be_unregistered_at_end() {
		
		ConnectionManagement connectionManagement = ConnectionManagement.getInstance();
		connectionManagement.addConnection("localhost", 111);
		connectionManagement.removeConnection("localhost", 111);

		assertThat(connectionManagement.isConnectionRegistered("localhost", 111), is(false));
		
	}
	
	@Test
	public void multiple_connections_with_same_description_should_be_registered() {
		
		ConnectionManagement connectionManagement = ConnectionManagement.getInstance();
		connectionManagement.addConnection("localhost", 111);
		connectionManagement.addConnection("localhost", 111);
		connectionManagement.removeConnection("localhost", 111);
		
		assertThat(connectionManagement.isConnectionRegistered("localhost", 111), is(true));
		
		connectionManagement.removeConnection("localhost", 111);
		assertThat(connectionManagement.isConnectionRegistered("localhost", 111), is(false));
		
	}
	
}
