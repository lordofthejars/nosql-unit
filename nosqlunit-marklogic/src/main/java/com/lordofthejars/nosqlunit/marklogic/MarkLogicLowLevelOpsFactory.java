package com.lordofthejars.nosqlunit.marklogic;

public class MarkLogicLowLevelOpsFactory {

    private static MarkLogicLowLevelOps marklogicLowLevelOps = null;

    public static final MarkLogicLowLevelOps getInstance() {
        if (marklogicLowLevelOps == null) {
            marklogicLowLevelOps = new MarkLogicLowLevelOps();
        }
        return marklogicLowLevelOps;
    }
}
