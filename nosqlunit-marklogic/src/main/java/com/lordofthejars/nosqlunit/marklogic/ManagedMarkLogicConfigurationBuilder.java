package com.lordofthejars.nosqlunit.marklogic;

import static com.lordofthejars.nosqlunit.marklogic.MarkLogicConfiguration.DEFAULT_HOST;


public class ManagedMarkLogicConfigurationBuilder extends MarkLogicConfigurationBuilder {

    private ManagedMarkLogicConfigurationBuilder() {
        marklogicConfiguration.setHost(DEFAULT_HOST);
    }

    public static ManagedMarkLogicConfigurationBuilder marklogic() {
        return new ManagedMarkLogicConfigurationBuilder();
    }
}
