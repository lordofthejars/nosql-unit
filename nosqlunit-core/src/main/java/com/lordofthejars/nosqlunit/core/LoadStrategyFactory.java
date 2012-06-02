package com.lordofthejars.nosqlunit.core;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class LoadStrategyFactory {

	public LoadStrategyFactory() {
		super();
	}
	
	public LoadStrategyOperation getLoadStrategyInstance(LoadStrategyEnum loadStrategyEnum, DatabaseOperation databaseOperation) {
		
		try {
			Constructor<? extends LoadStrategyOperation> constructor = loadStrategyEnum.loadStrategy().getConstructor(DatabaseOperation.class);
			constructor.setAccessible(true);
			
			return constructor.newInstance(databaseOperation);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(e);
		} catch (InstantiationException e) {
			throw new IllegalArgumentException(e);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(e);
		} catch (InvocationTargetException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
