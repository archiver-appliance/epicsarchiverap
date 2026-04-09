/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.model.SampleBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WriterRunnable is scheduled by the executor in the engine context every writing period.
 * @author Luofeng Li
 *
 */
public class WriterRunnable implements Runnable {
    private static final Logger logger = LogManager.getLogger(WriterRunnable.class);
    /** Minimum write period [seconds] */
    private static final double MIN_WRITE_PERIOD = 1.0;
    /** the sample buffer hash map */
    private final ConcurrentHashMap<String, SampleBuffer> buffers = new ConcurrentHashMap<>();

    /** the configservice used by this WriterRunnable */
    private final ConfigService configservice;
    /** guards against concurrent write() invocations */
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    /** virtual thread executor for parallel per-channel I/O */
    private final ExecutorService writeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    /** tracks in-flight year-change write futures per channel to prevent duplicate submission */
    private final ConcurrentHashMap<String, Future<?>> yearChangeFutures = new ConcurrentHashMap<>();
    /** optional semaphore capping concurrent channel writes; null means unlimited */
    private final Semaphore writeSemaphore;

    /**
     * the constructor
     * @param configservice the configservice used by this WriterRunnable
     */
    public WriterRunnable(ConfigService configservice) {
        this.configservice = configservice;
        int limit = Integer.parseInt(configservice
                .getInstallationProperties()
                .getProperty("org.epics.archiverappliance.engine.epics.writeThreadCount", "0"));
        this.writeSemaphore = (limit > 0) ? new Semaphore(limit) : null;
    }

    /** Add a channel's buffer that this thread reads
     * @param channel ArchiveChannel
     */
    public void addChannel(final ArchiveChannel channel) {
        addSampleBuffer(channel.getName(), channel.getSampleBuffer());
    }

    /**
     * remove one sample buffer from the buffer hash map.
     * At the same time. it also removes the channel from the channel hash map in the engine context
     * @param channelName the name of the channel who and whose sample buffer are removed
     */
    public void removeChannel(final String channelName) {
        SampleBuffer buffer = buffers.get(channelName);
        if (buffer != null) {
            ConcurrentHashMap<String, ArchiveChannel> channelList =
                    configservice.getEngineContext().getChannelList();
            buffer.resetSamples();
            ArrayListEventStream previousSamples = buffer.getPreviousSamples();
            if (!previousSamples.isEmpty()) {
                ArchiveChannel channel = channelList.get(channelName);
                if (channel != null) {
                    try (BasicContext ctx = new BasicContext()) {
                        channel.setlastRotateLogsEpochSeconds(System.currentTimeMillis() / 1000);
                        channel.getWriter().appendData(ctx, channelName, previousSamples);
                    } catch (IOException e) {
                        logger.error("Exception flushing buffer on channel removal for " + channelName, e);
                    }
                }
            }
        }
        buffers.remove(channelName);
    }

    /**
     * add sample buffer into this writer runnable and add year listener to each sample buffer
     * @param name the name of the channel
     * @param buffer the sample buffer for this channel
     */
    void addSampleBuffer(final String name, final SampleBuffer buffer) {
        buffers.put(name, buffer);
        buffer.addYearListener(this::writeYearChange);
    }

    /**
     * Validates and returns the effective write period, enforcing the minimum.
     * The minimum of {@value MIN_WRITE_PERIOD} second guards against excessive file
     * open/write/close frequency across all channels. With parallel virtual-thread
     * writes the write cycle itself completes quickly, so this floor is about
     * storage I/O frequency rather than write-cycle duration.
     * @param write_period  the requested writing period in seconds
     * @return the actual writing period in seconds (at least {@value MIN_WRITE_PERIOD})
     */
    public double setWritingPeriod(double write_period) {
        double tempwrite_period = write_period;
        if (tempwrite_period < MIN_WRITE_PERIOD) {
            tempwrite_period = MIN_WRITE_PERIOD;
        }
        return tempwrite_period;
    }

    @Override
    public void run() {
        try {
            long startMillis = System.currentTimeMillis();
            WriteCycleMetrics metrics = write();
            long wallClockMillis = System.currentTimeMillis() - startMillis;
            if (metrics == null) {
                configservice.getEngineContext().recordSkippedWriteCycle();
            } else {
                configservice
                        .getEngineContext()
                        .recordWriteCycle(
                                wallClockMillis / 1000.0,
                                metrics.totalChannelIOMillis() / 1000.0,
                                metrics.channelsWritten());
            }
        } catch (Exception e) {
            logger.error("Exception", e);
        }
    }

    /**
     * Flush a single channel's sample buffer on a year boundary.
     * Submits the blocking I/O to the virtual thread executor. Skips submission if a
     * year-change write for this channel is already in-flight, preventing duplicate writes.
     * @param buffer the sample buffer to be written
     */
    private void writeYearChange(SampleBuffer buffer) {
        if (writeExecutor.isShutdown()) {
            logger.warn("Skipping year-change flush for {} — executor already shut down", buffer.getChannelName());
            return;
        }
        String channelName = buffer.getChannelName();
        ConcurrentHashMap<String, ArchiveChannel> channelList =
                configservice.getEngineContext().getChannelList();

        buffer.resetSamples();
        ArrayListEventStream previousSamples = buffer.getPreviousSamples();
        if (previousSamples.isEmpty()) return;

        ArchiveChannel channel = channelList.get(channelName);
        if (channel == null) return;

        Future<?> existing = yearChangeFutures.get(channelName);
        if (existing != null && !existing.isDone()) {
            logger.debug("Year-change write already in-flight for {}; skipping duplicate", channelName);
            return;
        }

        channel.aboutToWriteBuffer((DBRTimeEvent) previousSamples.getLast());
        channel.setlastRotateLogsEpochSeconds(System.currentTimeMillis() / 1000);

        Future<?> future = writeExecutor.submit(() -> {
            try (BasicContext ctx = new BasicContext()) {
                channel.getWriter().appendData(ctx, channelName, previousSamples);
                logger.info(channelName + ": year change write complete");
            } catch (IOException e) {
                logger.error("Exception writing year-change buffer for " + channelName, e);
            } finally {
                yearChangeFutures.remove(channelName);
            }
        });
        yearChangeFutures.put(channelName, future);
    }

    /** Carries the data needed to write one channel's buffer in a single write cycle. */
    private record WriteTask(ArchiveChannel channel, String name, ArrayListEventStream samples) {}

    /** Summary of a completed write cycle returned to run() for metrics reporting. */
    private record WriteCycleMetrics(int channelsWritten, long totalChannelIOMillis) {}

    /**
     * Write all sample buffers into short term storage in parallel using Java 21 virtual threads.
     * All buffer swaps happen on the scheduler thread before fan-out so that every channel shares
     * a consistent epoch snapshot for the write cycle.
     * @return metrics for the completed cycle, or null if the cycle was skipped due to a prior cycle still running
     * @throws Exception error occurs during writing the sample buffer to the short term storage
     */
    private WriteCycleMetrics write() throws Exception {
        if (!isRunning.compareAndSet(false, true)) return null;
        try {
            final long writeTimestamp = System.currentTimeMillis() / 1000;
            ConcurrentHashMap<String, ArchiveChannel> channelList =
                    configservice.getEngineContext().getChannelList();

            List<WriteTask> tasks = collectWriteTasks(channelList, writeTimestamp);
            List<Future<Long>> futures = submitWriteTasks(tasks);
            long totalChannelIOMillis = awaitWriteCompletion(futures);
            return new WriteCycleMetrics(tasks.size(), totalChannelIOMillis);
        } finally {
            isRunning.set(false);
        }
    }

    /**
     * Swaps the double-buffer for every active channel on the scheduler thread, preparing a
     * snapshot of each channel's pending samples. All swaps happen before any I/O is submitted
     * so no channel accumulates new data into its previous buffer while another is still writing.
     */
    private List<WriteTask> collectWriteTasks(
            ConcurrentHashMap<String, ArchiveChannel> channelList, long writeTimestamp) {
        List<WriteTask> tasks = new ArrayList<>(buffers.size());
        for (Entry<String, SampleBuffer> entry : buffers.entrySet()) {
            SampleBuffer buffer = entry.getValue();
            if (!buffer.hasCurrentSamples()) continue;

            String channelName = buffer.getChannelName();
            buffer.resetSamples();
            ArrayListEventStream previousSamples = buffer.getPreviousSamples();
            if (previousSamples.isEmpty()) continue;

            ArchiveChannel channel = channelList.get(channelName);
            if (channel == null) continue;

            channel.aboutToWriteBuffer((DBRTimeEvent) previousSamples.getLast());
            channel.setlastRotateLogsEpochSeconds(writeTimestamp);
            tasks.add(new WriteTask(channel, channelName, previousSamples));
        }
        return tasks;
    }

    /**
     * Submits each channel's appendData() call to the virtual thread executor,
     * optionally throttled by the write semaphore.
     * Each future returns the elapsed wall-clock milliseconds for that channel's write.
     */
    private List<Future<Long>> submitWriteTasks(List<WriteTask> tasks) {
        List<Future<Long>> futures = new ArrayList<>(tasks.size());
        for (WriteTask task : tasks) {
            futures.add(writeExecutor.submit(() -> {
                if (writeSemaphore != null) writeSemaphore.acquireUninterruptibly();
                long t0 = System.currentTimeMillis();
                try (BasicContext ctx = new BasicContext()) {
                    task.channel().getWriter().appendData(ctx, task.name(), task.samples());
                } catch (IOException e) {
                    logger.error("Exception writing channel " + task.name(), e);
                } finally {
                    if (writeSemaphore != null) writeSemaphore.release();
                }
                return System.currentTimeMillis() - t0;
            }));
        }
        return futures;
    }

    /**
     * Waits for all submitted write futures to complete and returns the sum of per-channel
     * elapsed times in milliseconds. Joining here preserves backpressure on the scheduler:
     * if I/O is slow the next scheduled tick will observe isRunning=true and skip, rather
     * than queuing up unbounded work.
     */
    private long awaitWriteCompletion(List<Future<Long>> futures) throws Exception {
        long total = 0;
        for (Future<Long> future : futures) {
            total += future.get();
        }
        return total;
    }

    /**
     * flush out the sample buffer to the short term storage before shutting down the engine
     * @throws Exception  error occurs during writing the sample buffer to the short term storage
     */
    public void flushBuffer() throws Exception {
        write(); // metrics from this flush cycle are intentionally discarded
    }

    /**
     * Shut down the virtual thread executor, waiting up to 30 seconds for in-flight writes to complete.
     */
    public void shutdown() {
        writeExecutor.shutdown();
        try {
            if (!writeExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Write executor did not terminate within 30 seconds; forcing shutdown.");
                writeExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            writeExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
