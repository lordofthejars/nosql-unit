package com.lordofthejars.nosqlunit.redis.embedded;

import static java.nio.ByteBuffer.wrap;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class ExpirationDatatypeOperations {

	public enum TtlState {
		EXPIRED, NOT_EXPIRED, NOT_MANAGED;
	}


	public static final Long NO_EXPIRATION = -1L;

	protected Map<ByteBuffer, Long> expirationsInMillis = new HashMap<ByteBuffer, Long>();

	public long remainingTime(byte[] key) {
		
		if(timedoutState(key) == TtlState.NOT_EXPIRED) {
			Long expirationTime = expirationsInMillis.get(wrap(key));
			return TimeUnit.SECONDS.convert(expirationTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
		}
		
		return -1L;
		
	}
	
	public void addExpirationTime(byte[] key, long duration, TimeUnit unit) {

		ByteBuffer wrappedKey = wrap(key);
		
		long durationInMillis = unit.toMillis(duration);
		expirationsInMillis.put(wrappedKey, System.currentTimeMillis() + durationInMillis);

	}

	public void addExpirationAt(byte[] key, long time, TimeUnit unit) {
		
		ByteBuffer wrappedKey = wrap(key);
		
		long timeInMillis = unit.toMillis(time);
		expirationsInMillis.put(wrappedKey, timeInMillis);

	}
	
	public TtlState timedoutState(byte[] key) {
		
		ByteBuffer wrappedKey = wrap(key);
		
		if(expirationsInMillis.containsKey(wrappedKey)) {
			
			Long ttl = expirationsInMillis.get(wrappedKey);
			
			if(NO_EXPIRATION == ttl) {
				return TtlState.NOT_MANAGED;
			} else {
				
				boolean isExpired = System.currentTimeMillis() > ttl;
				return isExpired ? TtlState.EXPIRED : TtlState.NOT_EXPIRED;				
				
			}
			
		} else {
			return TtlState.NOT_MANAGED;
		}
		
	}
	
	public boolean removeExpiration(byte[] key) {
		
		ByteBuffer wrappedKey = wrap(key);
		
		if(expirationsInMillis.containsKey(wrappedKey)) {
			expirationsInMillis.remove(wrappedKey);
			return true;
		}
		
		return false;
	}
	
	public void renameTtlKey(byte[] oldKey, byte[] newKey) {
		
		ByteBuffer wrappedOldKey = wrap(oldKey);
		
		if(this.expirationsInMillis.containsKey(wrappedOldKey)) {
			Long ttl = this.expirationsInMillis.get(wrappedOldKey);
			this.expirationsInMillis.put(wrap(newKey), ttl);
			this.expirationsInMillis.remove(wrappedOldKey);
		}
		
	}
	
}
