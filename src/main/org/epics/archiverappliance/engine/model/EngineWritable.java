package org.epics.archiverappliance.engine.model;

import org.epics.archiverappliance.Writer;

/**
 * Something that can be handled by the engine's writable thread.
 * @author mshankar
 *
 */
public interface EngineWritable {
	public String getName();
	public SampleBuffer getSampleBuffer();
	public void setlastRotateLogsEpochSeconds(long lastRotationTime);
	public Writer getWriter();
}
