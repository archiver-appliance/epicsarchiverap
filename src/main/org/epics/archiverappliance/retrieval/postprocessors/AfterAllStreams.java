package org.epics.archiverappliance.retrieval.postprocessors;

import org.epics.archiverappliance.EventStream;

/**
 * If the post processor needs to send some data after all the streams have been processed, this is the hook.
 * This is often used to process the last known sample before the start time correctly
 * @author mshankar
 *
 */
public interface AfterAllStreams {
	public EventStream anyFinalData();
}
