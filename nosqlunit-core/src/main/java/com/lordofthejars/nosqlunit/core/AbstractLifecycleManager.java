package com.lordofthejars.nosqlunit.core;

import org.junit.rules.ExternalResource;


public abstract class AbstractLifecycleManager extends ExternalResource {


	public void before() throws Throwable {

		if (isServerNotStartedYet()) {
			doStart();
		}

		ConnectionManagement.getInstance().addConnection(getHost(), getPort());
	}


	public void after() {

		int remainingConnections = ConnectionManagement.getInstance().removeConnection(getHost(), getPort());

		if (noMoreConnectionsToManage(remainingConnections)) {
			doStop();
		}

	}

	private boolean noMoreConnectionsToManage(int remainingConnections) {
		return remainingConnections < 1;
	}

	private boolean isServerNotStartedYet() {
		return !ConnectionManagement.getInstance().isConnectionRegistered(getHost(), getPort());
	}

	protected abstract String getHost();

	protected abstract int getPort();

	protected abstract void doStart() throws Throwable;

	protected abstract void doStop();

}
