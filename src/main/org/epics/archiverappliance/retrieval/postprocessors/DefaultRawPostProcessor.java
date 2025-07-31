package org.epics.archiverappliance.retrieval.postprocessors;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Callable;

/**
 * This is the "default" post processor that does nothing to the underlying event stream.
 * @author mshankar
 *
 */
public class DefaultRawPostProcessor implements PostProcessor {
	public static final String PB_IDENTITY = "pb";
	

	@Override
	public void initialize(String userarg, String pvName) throws IOException {
	}

	@Override
    public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
		float storageRate = typeInfo.getComputedStorageRate();
		long numSeconds = TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start);
		// Add a fudge factor of 2 for java 
		long estimatedMemoryConsumption = (long) storageRate*numSeconds*2;
		return estimatedMemoryConsumption;
	}

	@Override
	public Callable<EventStream> wrap(Callable<EventStream> callable) {
		return callable;
	}

	@Override
	public String getIdentity() {
		return PB_IDENTITY;
	}

	@Override
	public String getExtension() {
		return PB_IDENTITY;
	}

}
