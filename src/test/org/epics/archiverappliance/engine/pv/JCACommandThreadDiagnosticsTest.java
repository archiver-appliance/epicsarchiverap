package org.epics.archiverappliance.engine.pv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class JCACommandThreadDiagnosticsTest {

    @Test
    void reportsCurrentAndQueuedCommandDetails() throws Exception {
        JCACommandThread thread = new JCACommandThread(3);
        CountDownLatch commandStarted = new CountDownLatch(1);
        CountDownLatch releaseCommand = new CountDownLatch(1);

        try {
            thread.start();
            thread.addCommand("pva.subscribe", "test:pva", () -> {
                commandStarted.countDown();
                try {
                    releaseCommand.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(commandStarted.await(5, TimeUnit.SECONDS));
            thread.addCommand("ca.connect", "test:ca", () -> {});
            Thread.sleep(30);

            Map<String, String> details = detailsByName(thread.getCommandThreadDetails());
            assertEquals("pva.subscribe", details.get("Current command label"));
            assertEquals("test:pva", details.get("Current command PV"));
            assertTrue(Long.parseLong(details.get("Current command running for (ms)")) > 0);
            assertTrue(Long.parseLong(details.get("Oldest queued command age (ms)")) > 0);

        } finally {
            releaseCommand.countDown();
            thread.shutdown();
        }

        Map<String, String> detailsAfterCompletion = detailsByName(thread.getCommandThreadDetails());
        assertEquals("3", detailsAfterCompletion.get("Commands executed"));
        assertTrue(Long.parseLong(detailsAfterCompletion.get("Last command completed at (epoch ms)")) > 0);
    }

    @Test
    void reportsQueueSnapshotWhileCommandsAreAdded() throws Exception {
        JCACommandThread thread = new JCACommandThread(4);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            thread.start();
            thread.addCommand("block", null, () -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            Thread producer = new Thread(() -> {
                for (int i = 0; i < 500; i++) {
                    thread.addCommand("queued", "test:pv", () -> {});
                }
            });
            producer.start();
            while (producer.isAlive()) {
                Map<String, String> details = detailsByName(thread.getCommandThreadDetails());
                int currentSize = Integer.parseInt(details.get("Current command queue size"));
                int maximumSize = Integer.parseInt(details.get("Max command queue size"));
                assertTrue(currentSize <= maximumSize);
                assertTrue(Long.parseLong(details.get("Oldest queued command age (ms)")) >= 0);
            }
            producer.join();
            Map<String, String> details = detailsByName(thread.getCommandThreadDetails());
            assertEquals("500", details.get("Current command queue size"));
            assertEquals("500", details.get("Max command queue size"));
        } finally {
            release.countDown();
            thread.shutdown();
        }
    }

    private static Map<String, String> detailsByName(List<Map<String, String>> details) {
        return details.stream()
                .collect(java.util.stream.Collectors.toMap(entry -> entry.get("name"), entry -> entry.get("value")));
    }
}
