package com.lordofthejars.nosqlunit.redis.embedded;

public class ConnectionServerOperations {

	/**
	 * Request for authentication in a password protected Redis server. A Redis
	 * server can be instructed to require a password before to allow clients to
	 * issue commands. This is done using the requirepass directive in the Redis
	 * configuration file. If the password given by the client is correct the
	 * server replies with an OK status code reply and starts accepting commands
	 * from the client. Otherwise an error is returned and the clients needs to
	 * try a new password. Note that for the high performance nature of Redis it
	 * is possible to try a lot of passwords in parallel in very short time, so
	 * make sure to generate a strong and very long password so that this attack
	 * is infeasible.
	 * 
	 * @param password
	 * @return Status code reply
	 */

	public String auth(final String password) {
		return "OK";
	}

	public byte[] echo(final byte[] string) {
		return string;
	}

	public String ping() {
		return "OK";
	}
	
	/**
     * Ask the server to silently close the connection.
     */
	public String quit() {
		return "OK";
	}
	
	/**
     * Select the DB with having the specified zero-based numeric index. For
     * default every new client connection is automatically selected to DB 0.
     * 
     * @param index
     * @return Status code reply
     */

    public String select(final int index) {
    	return "OK";
    }
	
}
