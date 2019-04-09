package com.lordofthejars.nosqlunit.marklogic;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import static com.lordofthejars.nosqlunit.env.SystemEnvironmentVariables.getPropertyVariable;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Ignores all tests if there is no Docker Container configured.
 */
public class DockerTestRunner extends BlockJUnit4ClassRunner {

    public DockerTestRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }

    @Override
    protected boolean isIgnored(FrameworkMethod child) {
        if (shouldIgnore(child)) {
            return true;
        }
        return super.isIgnored(child);
    }

    private boolean shouldIgnore(FrameworkMethod method) {
        if (method.getAnnotation(DockerTest.class) != null && getPropertyVariable("docker-container") == null) {
            getLogger(getClass()).info("Please configure the docker container name with -Ddocker-container=<your-marklogic-docker-container>");
            return true;
        }
        return false;
    }
}
