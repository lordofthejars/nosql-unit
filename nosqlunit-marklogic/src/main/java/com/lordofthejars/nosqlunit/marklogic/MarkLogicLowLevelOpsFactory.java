package com.lordofthejars.nosqlunit.marklogic;

public class MarkLogicLowLevelOpsFactory {

    private static MarkLogicLowLevelOps marklogicLowLevelOps = null;

    public static final MarkLogicLowLevelOps getSingletonInstance() {
        if (marklogicLowLevelOps == null) {
            marklogicLowLevelOps = new MarkLogicLowLevelOps();
        }
        return marklogicLowLevelOps;
    }
}
