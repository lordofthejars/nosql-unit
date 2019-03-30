package com.lordofthejars.nosqlunit.marklogic;


import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_APP_PORT;

public class RemoteMarkLogicConfigurationBuilder extends MarkLogicConfigurationBuilder {

    private RemoteMarkLogicConfigurationBuilder() {
        marklogicConfiguration.setPort(DEFAULT_APP_PORT);
    }

    public static RemoteMarkLogicConfigurationBuilder remoteMarkLogic() {
        return new RemoteMarkLogicConfigurationBuilder();
    }
}
