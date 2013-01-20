package com.lordofthejars.nosqlunit.core;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Test;

public class WhenEngineLifecycleIsManaged {

	@Test
	public void connection_should_be_registered() throws Throwable {
		
		MockLifecycle mockLifecycle = new MockLifecycle();
		mockLifecycle.startEngine();
		
		
		assertThat(mockLifecycle.number, is(1));
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("localhost", 0), is(true));
		
		mockLifecycle.stopEngine();
		
	}
	
	@Test
	public void connection_should_be_unregistered_at_the_end() throws Throwable {
		
		MockLifecycle mockLifecycle = new MockLifecycle();
		mockLifecycle.startEngine();
		
		mockLifecycle.stopEngine();
		
		
		assertThat(mockLifecycle.number, is(0));
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("localhost", 0), is(false));
		
	}
	
	@Test
	public void start_server_should_be_only_called_once() throws Throwable {
		
		MockLifecycle mockLifecycle = new MockLifecycle();
		mockLifecycle.startEngine();
		mockLifecycle.startEngine();
		
		assertThat(mockLifecycle.number, is(1));
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("localhost", 0), is(true));
		
		mockLifecycle.stopEngine();
		
		assertThat(mockLifecycle.number, is(1));
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("localhost", 0), is(true));
		
		mockLifecycle.stopEngine();
		
		
		assertThat(mockLifecycle.number, is(0));
		assertThat(ConnectionManagement.getInstance().isConnectionRegistered("localhost", 0), is(false));
		
	}

	private class MockLifecycle extends AbstractLifecycleManager {

		int number;
		
		@Override
		public String getHost() {
			return "localhost";
		}

		@Override
		public int getPort() {
			return 0;
		}

		@Override
		public void doStart() throws Throwable {
			number++;
		}

		@Override
		public void doStop() {
			number--;
		}
		
	}
	
}
