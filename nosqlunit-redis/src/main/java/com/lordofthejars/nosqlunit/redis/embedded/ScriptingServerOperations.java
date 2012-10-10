package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.List;

public class ScriptingServerOperations {

	/**
	 * Evaluates scripts using the Lua interpreter built into Redis starting
	 * from version 2.6.0.
	 * <p>
	 * 
	 * @return Script result
	 */
	public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
		throw new UnsupportedOperationException("Eval is not supported.");
	}

	public Object eval(byte[] script, byte[] keyCount, byte[][] params) {
		throw new UnsupportedOperationException("Eval is not supported.");
	}

	public Object evalsha(byte[] sha1, byte[] keyCount, byte[]... params) {
		throw new UnsupportedOperationException("Eval sha is not supported.");
	}

	public List<Long> scriptExists(byte[]... sha1) {
		throw new UnsupportedOperationException(
				"Script exists is not supported.");
	}

	public byte[] scriptFlush() {
		throw new UnsupportedOperationException(
				"Script flush is not supported.");
	}

	public byte[] scriptKill() {
		throw new UnsupportedOperationException("Script kill is not supported.");
	}

	public byte[] scriptLoad(byte[] script) {
		throw new UnsupportedOperationException("Script load is not supported.");
	}

	
}
