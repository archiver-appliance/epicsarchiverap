package org.epics.archiverappliance.engine.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.SampleBuffer;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.retrieval.channelarchiver.HashMapEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;


/**
 * A small test to test the double buffering algorithm we use. 
 * We reuse the SampleBuffer; but simulate the writer using the same approach that the WriterRunnable uses.
 * @author mshankar
 *
 */
@Tag("flaky")
public class DoubleBufferTest {
	private static Logger logger = LogManager.getLogger(DoubleBufferTest.class.getName());
	private int eventsAdded = 0;
	private int eventsStored = 0;


	enum StoredState { ALL_STORED, SOME_LOSS, MAYBE_SOME_LOSS}

    public static Stream<Arguments> data() {
        return Stream.of(
			Arguments.of(7, StoredState.SOME_LOSS),
			Arguments.of(8, StoredState.SOME_LOSS),
			Arguments.of(9, StoredState.SOME_LOSS),
			Arguments.of(10, StoredState.MAYBE_SOME_LOSS),
			Arguments.of(11, StoredState.ALL_STORED),
			Arguments.of(12, StoredState.ALL_STORED),
			Arguments.of(13, StoredState.ALL_STORED),
			Arguments.of(14, StoredState.ALL_STORED),
			Arguments.of(15, StoredState.ALL_STORED),
			Arguments.of(16, StoredState.ALL_STORED),
			Arguments.of(17, StoredState.ALL_STORED)
        );
    }


	@ParameterizedTest
	@MethodSource("data")
	public void testSampleBufferDoubleBufferingForBufferSize(int bufferSize, StoredState allStored) throws Exception {
		logger.info("Testing for buffer size " + bufferSize);
		this.eventsAdded = 0;
		this.eventsStored = 0;
		SampleBuffer buffer = new SampleBuffer("TestSampleBuffer", bufferSize, ArchDBRTypes.DBR_SCALAR_DOUBLE, new PVMetrics("TestSampleBuffer", null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
		ScheduledExecutorService dataGen = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "Data generator"));
		
		dataGen.scheduleAtFixedRate(() -> {
            try {
                HashMap<String, Object> eventAttrs = new HashMap<String, Object>();
                eventAttrs.put(HashMapEvent.SECS_FIELD_NAME, Long.toString(TimeUtils.getCurrentEpochSeconds()));
                eventAttrs.put(HashMapEvent.NANO_FIELD_NAME, "0");
                eventAttrs.put(HashMapEvent.VALUE_FIELD_NAME, TimeUtils.getCurrentEpochSeconds());
                buffer.add(new HashMapEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, eventAttrs));
                eventsAdded++;
            } catch(Exception ex) {
                ex.printStackTrace();
            }
        }, 0, 1, TimeUnit.MILLISECONDS);

		ScheduledExecutorService writer = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "Writer"));
		writer.scheduleAtFixedRate(() -> {
            buffer.resetSamples();
            ArrayListEventStream previousSamples = buffer.getPreviousSamples();
            try (BasicContext basicContext = new BasicContext()) {
                if (previousSamples.size() > 0) {
                    for(@SuppressWarnings("unused") Event event : previousSamples) {
                        eventsStored++;
                    }
                }
            } catch (IOException e) {
                logger.error("Exception saving data in test", e);
            }
        }, 10, 10, TimeUnit.MILLISECONDS);
		
		Thread.currentThread().sleep(1*1000);
		dataGen.shutdown();
		Thread.currentThread().sleep(1*1000);
		writer.shutdown();
		Thread.currentThread().sleep(1*1000);	
		
		Assertions.assertTrue(eventsAdded> 0 && eventsStored > 0, "No data generated for event count " + eventsAdded + " and stored events " + eventsStored);
		if(allStored == StoredState.ALL_STORED) {
            Assertions.assertEquals(eventsAdded, eventsStored, "Generated event count " + eventsAdded + " and stored events " + eventsStored + " are not the same for buffer size " + bufferSize);
		} else if(allStored == StoredState.SOME_LOSS) { 
			Assertions.assertTrue(eventsAdded > eventsStored, "Generated event count " + eventsAdded + " and stored events " + eventsStored + " must have some loss for buffer size " + bufferSize);
		}
	}
}
