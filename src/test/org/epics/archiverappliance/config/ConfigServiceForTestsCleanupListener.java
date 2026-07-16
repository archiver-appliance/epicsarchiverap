package org.epics.archiverappliance.config;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

/**
 * Shuts down test-driver {@link ConfigServiceForTests} instances when the test class that created
 * them finishes, so their engine threads (JCA command threads plus schedulers) do not accumulate
 * across the shared test JVM. Cross-class shared helpers opt out via
 * {@link ConfigServiceForTests#keepAliveAcrossTests()}.
 *
 * <p>Auto-registered via {@code META-INF/services/org.junit.platform.launcher.TestExecutionListener}.
 */
public class ConfigServiceForTestsCleanupListener implements TestExecutionListener {

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
        boolean isTestClass = testIdentifier.isContainer()
                && testIdentifier
                        .getSource()
                        .filter(ClassSource.class::isInstance)
                        .isPresent();
        if (isTestClass) {
            ConfigServiceForTests.shutdownTrackedInstances();
        }
    }
}
