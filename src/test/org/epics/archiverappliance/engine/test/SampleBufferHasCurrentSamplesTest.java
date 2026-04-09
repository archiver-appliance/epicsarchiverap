package org.epics.archiverappliance.engine.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.HashMapEvent;
import org.epics.archiverappliance.engine.model.SampleBuffer;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * Unit tests for {@link SampleBuffer#hasCurrentSamples()}, which is used by
 * WriterRunnable to skip idle channels without swapping their double-buffer.
 */
class SampleBufferHasCurrentSamplesTest {

    private static SampleBuffer makeBuffer(String name) {
        return new SampleBuffer(
                name,
                10,
                ArchDBRTypes.DBR_SCALAR_DOUBLE,
                new PVMetrics(name, null, -1, ArchDBRTypes.DBR_SCALAR_DOUBLE));
    }

    private static HashMapEvent makeEvent() {
        HashMap<String, Object> attrs = new HashMap<>();
        attrs.put(HashMapEvent.SECS_FIELD_NAME, Long.toString(TimeUtils.getCurrentEpochSeconds()));
        attrs.put(HashMapEvent.NANO_FIELD_NAME, "0");
        attrs.put(HashMapEvent.VALUE_FIELD_NAME, 1.0);
        return new HashMapEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE, attrs);
    }

    @Test
    void emptyBufferReturnsFalse() {
        assertFalse(
                makeBuffer("TEST:EMPTY").hasCurrentSamples(),
                "Freshly created buffer should report no current samples");
    }

    @Test
    void nonEmptyBufferReturnsTrue() {
        SampleBuffer buffer = makeBuffer("TEST:NONEMPTY");
        buffer.add(makeEvent());
        assertTrue(buffer.hasCurrentSamples(), "Buffer with one sample should report current samples present");
    }

    @Test
    void falseAfterResetSamples() {
        SampleBuffer buffer = makeBuffer("TEST:AFTERRESET");
        buffer.add(makeEvent());
        buffer.resetSamples(); // swaps: previous = [event], current = new empty
        assertFalse(buffer.hasCurrentSamples(), "After resetSamples() the fresh active buffer should be empty");
    }
}
