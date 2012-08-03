package com.lordofthejars.nosqlunit.core;

public enum LoadStrategyEnum {

	
	INSERT(InsertLoadStrategyOperation.class), CLEAN_INSERT(CleanInsertLoadStrategyOperation.class), DELETE_ALL(DeleteAllLoadStrategyOperation.class), @Deprecated REFRESH(RefreshLoadStrategyOperation.class);
	
	private final Class<? extends LoadStrategyOperation> strategyClass;
	
	private LoadStrategyEnum(Class<? extends LoadStrategyOperation> strategyClass) {
		this.strategyClass = strategyClass;
	}
	
	public Class<? extends LoadStrategyOperation> loadStrategy() {
		return strategyClass;
	}
}
