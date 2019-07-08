package com.lordofthejars.nosqlunit.influxdb;

import org.junit.rules.ExternalResource;

public class InMemoryInfluxDb extends ExternalResource {

    protected InMemoryInfluxDbLifecycleManager inMemoryInfluxDbLifecycleManager = null;

    private InMemoryInfluxDb() {
        super();
    }

    public static class InMemoryInfluxRuleBuilder {

        private final InMemoryInfluxDbLifecycleManager inMemoryInfluxDbLifecycleManager;

        private InMemoryInfluxRuleBuilder() {
            this.inMemoryInfluxDbLifecycleManager = new InMemoryInfluxDbLifecycleManager();
        }

        public static InMemoryInfluxRuleBuilder newInMemoryInfluxDbRule() {
            return new InMemoryInfluxRuleBuilder();
        }

        public InMemoryInfluxRuleBuilder targetPath(final String targetPath) {
            this.inMemoryInfluxDbLifecycleManager.setTargetPath(targetPath);
            return this;
        }


        public InMemoryInfluxDb build() {

            if(this.inMemoryInfluxDbLifecycleManager.getTargetPath() == null) {
                throw new IllegalArgumentException("No Path to Embedded Influxdb is provided.");
            }

            final InMemoryInfluxDb inMemoryInfluxDb = new InMemoryInfluxDb();
            inMemoryInfluxDb.inMemoryInfluxDbLifecycleManager = this.inMemoryInfluxDbLifecycleManager;

            return inMemoryInfluxDb;

        }

    }

    @Override
    public void before() throws Throwable {
        inMemoryInfluxDbLifecycleManager.startEngine();
    }

    @Override
    public void after() {
        inMemoryInfluxDbLifecycleManager.stopEngine();
    }


}
