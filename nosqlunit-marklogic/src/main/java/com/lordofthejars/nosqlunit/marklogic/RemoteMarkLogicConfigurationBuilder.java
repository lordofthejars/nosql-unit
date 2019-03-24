package com.lordofthejars.nosqlunit.marklogic;


public class RemoteMarkLogicConfigurationBuilder extends MarkLogicConfigurationBuilder {

    private static final int DEFAULT_PORT = MarkLogicConfiguration.DEFAULT_APP_PORT;

    private RemoteMarkLogicConfigurationBuilder() {
        marklogicConfiguration.setPort(DEFAULT_PORT);
    }

    public static RemoteMarkLogicConfigurationBuilder remoteMarkLogic() {
        return new RemoteMarkLogicConfigurationBuilder();
    }
}
