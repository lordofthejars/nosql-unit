package com.lordofthejars.nosqlunit.core;

import java.util.HashMap;
import java.util.Map;

public final class ConnectionManagement {

	private final class Connection {
		
		private final String host;
		private final int port;
		
		public Connection(String host, int port) {
			super();
			this.host = host;
			this.port = port;
		}

		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((host == null) ? 0 : host.hashCode());
			result = prime * result + port;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Connection other = (Connection) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (host == null) {
				if (other.host != null)
					return false;
			} else if (!host.equals(other.host))
				return false;
			if (port != other.port)
				return false;
			return true;
		}

		private ConnectionManagement getOuterType() {
			return ConnectionManagement.this;
		}
		
		
	}
	
	private static ConnectionManagement connectionManagement;

	private Map<Connection, Integer> currentConnections = new HashMap<ConnectionManagement.Connection, Integer>();  

	
	private ConnectionManagement() {
		super();
	}

	public static synchronized ConnectionManagement getInstance() {
		if (connectionManagement == null) {
			connectionManagement = new ConnectionManagement();
		}
		return connectionManagement;
	}

	public void addConnection(String host, int port) {
		
		Connection connection = new Connection(host, port);
		
		if(isConnectionRegistered(connection)) {
			int previousNumberOfConnections = currentConnections.remove(connection);
			currentConnections.put(connection, previousNumberOfConnections+1);
		} else {
			currentConnections.put(new Connection(host, port), 1);
		}
	}
	
	public int removeConnection(String host, int port) {
		
		Connection connection = new Connection(host, port);
		
		if(isConnectionRegistered(connection)) {
			int previousNumberOfConnections = currentConnections.remove(connection);
			int numberCurrentConnections = previousNumberOfConnections-1;
			
			if(numberCurrentConnections > 0) {
				this.currentConnections.put(connection, numberCurrentConnections);
			}
			
			return numberCurrentConnections;
		}
		
		return 0;
	}
	
	public boolean isConnectionRegistered(String host, int port) {
		return isConnectionRegistered(new Connection(host, port));
	}
	
	private boolean isConnectionRegistered(Connection connection) {
		return currentConnections.containsKey(connection);
	}
}
