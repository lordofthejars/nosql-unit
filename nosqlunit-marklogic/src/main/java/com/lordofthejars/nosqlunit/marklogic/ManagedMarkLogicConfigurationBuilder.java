package com.lordofthejars.nosqlunit.marklogic;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_APP_PORT;
import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_HOST;


public class ManagedMarkLogicConfigurationBuilder extends MarkLogicConfigurationBuilder {

    private ManagedMarkLogicConfigurationBuilder() {
        marklogicConfiguration.setHost(DEFAULT_HOST);
        marklogicConfiguration.setPort(DEFAULT_APP_PORT);
    }

    public static ManagedMarkLogicConfigurationBuilder marklogic() {
        return new ManagedMarkLogicConfigurationBuilder();
    }
}
