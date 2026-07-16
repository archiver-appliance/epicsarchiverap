package org.epics.archiverappliance.engine.pv;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Deterministic guard for the engine CAJ-context teardown fix.
 *
 * <p>In the shared-JVM integration tests the engine webapp is deployed and undeployed many times in
 * one JVM. Each deploy starts {@code commandThreadCount} (default 10) {@link JCACommandThread}s,
 * each owning a CAJ context. {@link JCACommandThread#shutdown()} must therefore terminate the thread
 * and destroy its context <em>synchronously</em> - if it returns while the thread is still alive,
 * Tomcat reports a leaked thread on undeploy and the threads accumulate across test classes.
 *
 * <p>This is a unit test (no Tomcat, no EPICS/soft-IOC): a {@code CAJContext} can be created and
 * destroyed in-process. It directly asserts the {@code shutdown()} contract rather than measuring
 * JVM-wide thread counts, which are confounded by any CA server reachable from the test host.
 */
public class JCACommandThreadShutdownTest {

    @Test
    public void shutdownTerminatesThreadAndDestroysContext() throws Exception {
        JCACommandThread commandThread = new JCACommandThread(0);
        commandThread.start();
        assertTrue(commandThread.isAlive(), "Command thread should be running after start()");

        commandThread.shutdown();

        // shutdown() must not return until the thread has actually exited and the CAJ context has
        // been destroyed - otherwise the thread (and its CAJ network threads) leak on webapp undeploy.
        assertFalse(commandThread.isAlive(), "Command thread should be dead once shutdown() returns");
        assertTrue(commandThread.isContextDestroyed(), "CAJ context should be destroyed after shutdown()");
    }
}
