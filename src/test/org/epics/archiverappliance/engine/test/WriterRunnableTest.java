package org.epics.archiverappliance.engine.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.HashMapEvent;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.model.SampleBuffer;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.engine.writer.WriterRunnable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link WriterRunnable} covering parallelism, concurrency safety,
 * and correctness.
 */
class WriterRunnableTest {

    /** Number of channels for the parallelism timing test */
    private static final int PARALLEL_CHANNEL_COUNT = 200;
    /** Channels and samples used for the data-integrity test */
    private static final int DATA_CHANNEL_COUNT = 100;

    private static final int SAMPLES_PER_CHANNEL = 20;
    /** Per-channel I/O delay used in timing-sensitive tests */
    private static final long WRITE_DELAY_MS = 50;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds the mocked ConfigService + EngineContext that WriterRunnable requires.
     *
     * @param channelList    the channel map the engine context will return
     * @param writeThreadCount  0 = unlimited (no semaphore), >0 caps concurrency
     */
    private ConfigService buildConfigService(
            ConcurrentHashMap<String, ArchiveChannel> channelList, int writeThreadCount) {
        EngineContext engineContext = mock(EngineContext.class);
        when(engineContext.getChannelList()).thenReturn(channelList);
        when(engineContext.getWriteThreadCount()).thenReturn(writeThreadCount);

        Properties props = new Properties();
        props.setProperty(
                "org.epics.archiverappliance.engine.epics.writeThreadCount", Integer.toString(writeThreadCount));

        ConfigService configService = mock(ConfigService.class);
        when(configService.getEngineContext()).thenReturn(engineContext);
        when(configService.getInstallationProperties()).thenReturn(props);
        return configService;
    }

    private HashMapEvent makeEvent() {
        return makeEventAt(TimeUtils.getCurrentEpochSeconds());
    }

    /**
     * Creates an event with a specific EPICS-epoch timestamp and a value equal to that
     * timestamp, so both fields can be used to assert event identity at the write site.
     */
    private HashMapEvent makeEventAt(long epicsSecs) {
        HashMap<String, Object> attrs = new HashMap<>();
        attrs.put(HashMapEvent.SECS_FIELD_NAME, Long.toString(epicsSecs));
        attrs.put(HashMapEvent.NANO_FIELD_NAME, "0");
        attrs.put(HashMapEvent.VALUE_FIELD_NAME, (double) epicsSecs);
        return new HashMapEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, attrs);
    }

    /**
     * Returns an event timestamped at the start of the next calendar year in EPICS epoch seconds.
     * Adding this to a buffer that has already seen a current-year event will trigger the year
     * listener.
     */
    private HashMapEvent makeNextYearEvent() {
        int nextYear = LocalDate.now(ZoneOffset.UTC).getYear() + 1;
        long javaEpochSecs =
                LocalDate.of(nextYear, 1, 1).atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        // EPICS epoch starts Jan 1, 1990; subtract its Unix timestamp to get EPICS seconds
        long epicsEpochOffset = Instant.parse("1990-01-01T00:00:00Z").getEpochSecond();
        long epicsEpochSecs = javaEpochSecs - epicsEpochOffset;

        HashMap<String, Object> attrs = new HashMap<>();
        attrs.put(HashMapEvent.SECS_FIELD_NAME, Long.toString(epicsEpochSecs));
        attrs.put(HashMapEvent.NANO_FIELD_NAME, "0");
        attrs.put(HashMapEvent.VALUE_FIELD_NAME, 99.0);
        return new HashMapEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, attrs);
    }

    /** A Writer that sleeps WRITE_DELAY_MS per call to simulate blocking I/O. */
    static class SlowCountingWriter implements Writer {
        private final AtomicInteger writeCount = new AtomicInteger(0);

        @Override
        public int appendData(BasicContext context, String pvName, EventStream stream) throws IOException {
            try {
                Thread.sleep(WRITE_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            int count = 0;
            for (@SuppressWarnings("unused") Event e : stream) count++;
            writeCount.incrementAndGet();
            return count;
        }

        @Override
        public Event getLastKnownEvent(BasicContext context, String pvName) {
            return null;
        }

        int getWriteCount() {
            return writeCount.get();
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Channels with no buffered samples must not trigger appendData.
     * Verifies that hasCurrentSamples() correctly gates the buffer swap and I/O submission.
     * Uses 40 channels (20 active, 20 idle) to give the skip logic meaningful coverage.
     */
    @Test
    void testIdleChannelsSkipped() {
        int activeCount = 20;
        int idleCount = 20;
        Set<String> channelsWritten = ConcurrentHashMap.newKeySet();

        Writer trackingWriter = new Writer() {
            @Override
            public int appendData(BasicContext ctx, String pvName, EventStream stream) {
                channelsWritten.add(pvName);
                for (@SuppressWarnings("unused") Event e : stream) {}
                return 0;
            }

            @Override
            public Event getLastKnownEvent(BasicContext ctx, String pvName) {
                return null;
            }
        };

        Set<String> expectedActive = new HashSet<>();
        ConcurrentHashMap<String, ArchiveChannel> channelList = new ConcurrentHashMap<>();
        WriterRunnable writerRunnable = new WriterRunnable(buildConfigService(channelList, 0));

        for (int i = 0; i < activeCount + idleCount; i++) {
            String name = "TEST:PV:" + i;
            SampleBuffer buffer = new SampleBuffer(
                    name,
                    10,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
            if (i < activeCount) {
                buffer.add(makeEvent());
                expectedActive.add(name);
            }

            ArchiveChannel channel = mock(ArchiveChannel.class);
            when(channel.getName()).thenReturn(name);
            when(channel.getSampleBuffer()).thenReturn(buffer);
            when(channel.getWriter()).thenReturn(trackingWriter);
            channelList.put(name, channel);
            writerRunnable.addChannel(channel);
        }

        writerRunnable.run();
        writerRunnable.shutdown();

        assertEquals(
                expectedActive,
                channelsWritten,
                "appendData must be called for exactly the " + activeCount + " active channels — no more, no fewer");
    }

    /**
     * Every event buffered for every channel must arrive at the writer with its identity
     * intact. Uses DATA_CHANNEL_COUNT channels × SAMPLES_PER_CHANNEL events each.
     * Each event carries a unique EPICS timestamp; the writer collects the timestamps it
     * sees per channel and we assert exact set equality against what was buffered.
     */
    @Test
    void testAllActiveChannelsWritten() {
        // base EPICS time; each event gets base + channel_index * stride + sample_index
        // so all timestamps are unique across the entire test and within the current year.
        long base = TimeUtils.getCurrentEpochSeconds();
        int stride = SAMPLES_PER_CHANNEL + 1; // gap between channels avoids overlap

        Map<String, Set<Instant>> expected = new HashMap<>();
        ConcurrentHashMap<String, Set<Instant>> actual = new ConcurrentHashMap<>();

        Writer verifyingWriter = new Writer() {
            @Override
            public int appendData(BasicContext ctx, String pvName, EventStream stream) {
                Set<Instant> written = actual.computeIfAbsent(pvName, k -> ConcurrentHashMap.newKeySet());
                int count = 0;
                for (Event e : stream) {
                    written.add(e.getEventTimeStamp());
                    count++;
                }
                return count;
            }

            @Override
            public Event getLastKnownEvent(BasicContext ctx, String pvName) {
                return null;
            }
        };

        ConcurrentHashMap<String, ArchiveChannel> channelList = new ConcurrentHashMap<>();
        WriterRunnable writerRunnable = new WriterRunnable(buildConfigService(channelList, 0));

        for (int i = 0; i < DATA_CHANNEL_COUNT; i++) {
            String name = "TEST:PV:" + i;
            SampleBuffer buffer = new SampleBuffer(
                    name,
                    SAMPLES_PER_CHANNEL + 5,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));

            Set<Instant> channelExpected = new HashSet<>();
            for (int s = 0; s < SAMPLES_PER_CHANNEL; s++) {
                HashMapEvent event = makeEventAt(base + (long) i * stride + s);
                buffer.add(event);
                channelExpected.add(event.getEventTimeStamp());
            }
            expected.put(name, channelExpected);

            ArchiveChannel channel = mock(ArchiveChannel.class);
            when(channel.getName()).thenReturn(name);
            when(channel.getSampleBuffer()).thenReturn(buffer);
            when(channel.getWriter()).thenReturn(verifyingWriter);
            channelList.put(name, channel);
            writerRunnable.addChannel(channel);
        }

        writerRunnable.run();
        writerRunnable.shutdown();

        assertEquals(DATA_CHANNEL_COUNT, actual.size(), "Every channel must have been written");
        for (Map.Entry<String, Set<Instant>> entry : expected.entrySet()) {
            assertEquals(
                    entry.getValue(),
                    actual.get(entry.getKey()),
                    "Events for " + entry.getKey()
                            + " must be written exactly as buffered — no loss, no duplication, no corruption");
        }
    }

    /**
     * With PARALLEL_CHANNEL_COUNT channels each requiring WRITE_DELAY_MS of I/O, the
     * total wall-clock time must be far below the sequential upper bound, proving that
     * virtual threads run the writes concurrently.
     */
    @Test
    void testWritesRunInParallel() {
        SlowCountingWriter slowWriter = new SlowCountingWriter();
        ConcurrentHashMap<String, ArchiveChannel> channelList = new ConcurrentHashMap<>();
        WriterRunnable writerRunnable = new WriterRunnable(buildConfigService(channelList, 0));

        for (int i = 0; i < PARALLEL_CHANNEL_COUNT; i++) {
            String name = "TEST:PV:" + i;
            SampleBuffer buffer = new SampleBuffer(
                    name,
                    10,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
            buffer.add(makeEvent());

            ArchiveChannel channel = mock(ArchiveChannel.class);
            when(channel.getName()).thenReturn(name);
            when(channel.getSampleBuffer()).thenReturn(buffer);
            when(channel.getWriter()).thenReturn(slowWriter);
            channelList.put(name, channel);
            writerRunnable.addChannel(channel);
        }

        long start = System.currentTimeMillis();
        writerRunnable.run();
        long elapsed = System.currentTimeMillis() - start;
        writerRunnable.shutdown();

        long sequentialMs = PARALLEL_CHANNEL_COUNT * WRITE_DELAY_MS;
        assertEquals(
                PARALLEL_CHANNEL_COUNT,
                slowWriter.getWriteCount(),
                "All " + PARALLEL_CHANNEL_COUNT + " channels must be written");
        assertTrue(
                elapsed < sequentialMs,
                "Parallel writes took " + elapsed + "ms; sequential would take ~" + sequentialMs
                        + "ms — expected much less");
    }

    /**
     * A second concurrent run() call while the first is still executing must be silently dropped
     * by the AtomicBoolean reentrancy guard.
     */
    @Test
    void testReentrancyGuardPreventsOverlap() throws Exception {
        AtomicInteger appendDataCallCount = new AtomicInteger(0);
        CountDownLatch firstRunStarted = new CountDownLatch(1);

        Writer blockingWriter = new Writer() {
            @Override
            public int appendData(BasicContext ctx, String pvName, EventStream stream) throws IOException {
                appendDataCallCount.incrementAndGet();
                firstRunStarted.countDown();
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // Consume the event stream
                for (@SuppressWarnings("unused") Event e : stream) {}
                return 0;
            }

            @Override
            public Event getLastKnownEvent(BasicContext ctx, String pvName) {
                return null;
            }
        };

        String name = "TEST:PV:0";
        SampleBuffer buffer = new SampleBuffer(
                name,
                10,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
        buffer.add(makeEvent());

        ArchiveChannel channel = mock(ArchiveChannel.class);
        when(channel.getName()).thenReturn(name);
        when(channel.getSampleBuffer()).thenReturn(buffer);
        when(channel.getWriter()).thenReturn(blockingWriter);

        ConcurrentHashMap<String, ArchiveChannel> channelList = new ConcurrentHashMap<>();
        channelList.put(name, channel);
        WriterRunnable writerRunnable = new WriterRunnable(buildConfigService(channelList, 0));
        writerRunnable.addChannel(channel);

        // Start the first run in a virtual thread so it blocks in appendData
        Thread firstRun = Thread.ofVirtual().start(() -> {
            try {
                writerRunnable.run();
            } catch (Exception ignored) {
            }
        });

        // Wait until the first run is inside appendData, then fire a second run
        assertTrue(firstRunStarted.await(5, TimeUnit.SECONDS), "First run should have started within 5 seconds");
        writerRunnable.run(); // must return immediately — isRunning is true

        firstRun.join(5000);
        writerRunnable.shutdown();

        assertFalse(firstRun.isAlive(), "First run should have completed");
        assertEquals(
                1,
                appendDataCallCount.get(),
                "reentrancy guard must suppress the second run() — appendData called only once");
    }

    /**
     * When writeThreadCount > 0, the Semaphore must cap the number of channel writes
     * executing concurrently to that limit.
     */
    @Test
    void testSemaphoreCapsMaxConcurrency() {
        int cap = 3;
        AtomicInteger concurrentWrites = new AtomicInteger(0);
        AtomicInteger maxConcurrentWrites = new AtomicInteger(0);

        Writer cappedWriter = new Writer() {
            @Override
            public int appendData(BasicContext ctx, String pvName, EventStream stream) throws IOException {
                int current = concurrentWrites.incrementAndGet();
                maxConcurrentWrites.updateAndGet(m -> Math.max(m, current));
                try {
                    Thread.sleep(WRITE_DELAY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                concurrentWrites.decrementAndGet();
                // Consume the event stream
                for (@SuppressWarnings("unused") Event e : stream) {}
                return 0;
            }

            @Override
            public Event getLastKnownEvent(BasicContext ctx, String pvName) {
                return null;
            }
        };

        ConcurrentHashMap<String, ArchiveChannel> channelList = new ConcurrentHashMap<>();
        // writeThreadCount = cap → semaphore limits to `cap` concurrent writes
        WriterRunnable writerRunnable = new WriterRunnable(buildConfigService(channelList, cap));

        for (int i = 0; i < 20; i++) {
            String name = "TEST:PV:" + i;
            SampleBuffer buffer = new SampleBuffer(
                    name,
                    10,
                    ArchDBRTypes.DBR_SCALAR_DOUBLE,
                    new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
            buffer.add(makeEvent());

            ArchiveChannel channel = mock(ArchiveChannel.class);
            when(channel.getName()).thenReturn(name);
            when(channel.getSampleBuffer()).thenReturn(buffer);
            when(channel.getWriter()).thenReturn(cappedWriter);
            channelList.put(name, channel);
            writerRunnable.addChannel(channel);
        }

        writerRunnable.run();
        writerRunnable.shutdown();

        assertTrue(
                maxConcurrentWrites.get() <= cap,
                "Semaphore should limit concurrent writes to " + cap + ", but observed " + maxConcurrentWrites.get());
    }

    /**
     * Verifies that the year-change write path calls aboutToWriteBuffer().
     * <p>
     * The year listener is triggered by adding an event whose timestamp crosses the year
     * boundary. aboutToWriteBuffer() is called synchronously before I/O is submitted, so
     * no extra waiting is required to verify the call.
     */
    @Test
    void testYearChangeCallsAboutToWriteBuffer() throws Exception {
        CountDownLatch writeDone = new CountDownLatch(1);
        Writer latchWriter = new Writer() {
            @Override
            public int appendData(BasicContext ctx, String pvName, EventStream stream) {
                for (@SuppressWarnings("unused") Event e : stream) {}
                writeDone.countDown();
                return 0;
            }

            @Override
            public Event getLastKnownEvent(BasicContext ctx, String pvName) {
                return null;
            }
        };

        String name = "TEST:YEARCHANGE:PV";
        SampleBuffer buffer = new SampleBuffer(
                name,
                10,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));

        ArchiveChannel channel = mock(ArchiveChannel.class);
        when(channel.getName()).thenReturn(name);
        when(channel.getSampleBuffer()).thenReturn(buffer);
        when(channel.getWriter()).thenReturn(latchWriter);

        ConcurrentHashMap<String, ArchiveChannel> channelList = new ConcurrentHashMap<>();
        channelList.put(name, channel);
        WriterRunnable writerRunnable = new WriterRunnable(buildConfigService(channelList, 0));
        writerRunnable.addChannel(channel);

        // Seed the buffer with a current-year event so the buffer's year is set
        buffer.add(makeEvent());
        // Adding a next-year event triggers the year listener → writeYearChange
        buffer.add(makeNextYearEvent());

        // Wait for the async appendData to complete (confirms the write was actually submitted)
        assertTrue(writeDone.await(5, TimeUnit.SECONDS), "Year-change write should complete within 5 seconds");
        writerRunnable.shutdown();

        // aboutToWriteBuffer is called synchronously in writeYearChange before I/O is submitted
        verify(channel).aboutToWriteBuffer(any());
    }

    /**
     * Verifies that removeChannel flushes any buffered samples synchronously before
     * the channel is removed, so no data is lost on archival stop.
     */
    @Test
    void testRemoveChannelFlushesSynchronously() {
        AtomicInteger appendCount = new AtomicInteger(0);
        Writer trackingWriter = new Writer() {
            @Override
            public int appendData(BasicContext ctx, String pvName, EventStream stream) {
                appendCount.incrementAndGet();
                return 0;
            }

            @Override
            public Event getLastKnownEvent(BasicContext ctx, String pvName) {
                return null;
            }
        };

        String name = "TEST:REMOVE:PV";
        SampleBuffer buffer = new SampleBuffer(
                name,
                10,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
        buffer.add(makeEvent());

        ArchiveChannel channel = mock(ArchiveChannel.class);
        when(channel.getName()).thenReturn(name);
        when(channel.getSampleBuffer()).thenReturn(buffer);
        when(channel.getWriter()).thenReturn(trackingWriter);

        ConcurrentHashMap<String, ArchiveChannel> channelList = new ConcurrentHashMap<>();
        channelList.put(name, channel);
        WriterRunnable writerRunnable = new WriterRunnable(buildConfigService(channelList, 0));
        writerRunnable.addChannel(channel);

        writerRunnable.removeChannel(name);
        writerRunnable.shutdown();

        assertEquals(
                1,
                appendCount.get(),
                "removeChannel must flush buffered data synchronously before removing the channel");
    }

    /**
     * Verifies that removeChannel does not call appendData when the buffer is empty,
     * avoiding a spurious zero-length write on removal.
     */
    @Test
    void testRemoveChannelSkipsFlushWhenEmpty() throws Exception {
        Writer writer = mock(Writer.class);

        String name = "TEST:REMOVE:EMPTY";
        SampleBuffer buffer = new SampleBuffer(
                name,
                10,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
        // No samples added — buffer is empty

        ArchiveChannel channel = mock(ArchiveChannel.class);
        when(channel.getName()).thenReturn(name);
        when(channel.getSampleBuffer()).thenReturn(buffer);
        when(channel.getWriter()).thenReturn(writer);

        ConcurrentHashMap<String, ArchiveChannel> channelList = new ConcurrentHashMap<>();
        channelList.put(name, channel);
        WriterRunnable writerRunnable = new WriterRunnable(buildConfigService(channelList, 0));
        writerRunnable.addChannel(channel);

        writerRunnable.removeChannel(name);
        writerRunnable.shutdown();

        verify(writer, never()).appendData(any(), any(), any());
    }
}
