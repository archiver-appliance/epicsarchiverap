package org.epics.archiverappliance.retrieval;

import java.io.IOException;

import org.epics.archiverappliance.config.ArchDBRTypes;

/**
 * Thrown when we have a mismatch in what config tells us and what a stream tells us.
 * A mismatch in DBR types can be a problem when transporting data using binary formats which are type sensitive.
 * Rather than silently lose precision, we throw an exception and perhaps ignore the stream.
 * @author mshankar
 *
 */
public class MismatchedDBRTypeException extends IOException {
	private static final long serialVersionUID = -8686424918669512613L;
	private String pvName;
	private ArchDBRTypes configDBRType;
	private String sourceDesc;
	private ArchDBRTypes streamDBRType;
	
	public MismatchedDBRTypeException(String pvName,ArchDBRTypes configDBRType, String sourceDesc, ArchDBRTypes streamDBRType) {
		super();
		this.pvName = pvName;
		this.configDBRType = configDBRType;
		this.sourceDesc = sourceDesc;
		this.streamDBRType = streamDBRType;
	}

	/**
	 * @return the pvName
	 */
	public String getPvName() {
		return pvName;
	}

	/**
	 * @return the configDBRType
	 */
	public ArchDBRTypes getConfigDBRType() {
		return configDBRType;
	}

	/**
	 * @return the sourceDesc
	 */
	public String getSourceDesc() {
		return sourceDesc;
	}

	/**
	 * @return the streamDBRType
	 */
	public ArchDBRTypes getStreamDBRType() {
		return streamDBRType;
	}

	/* (non-Javadoc)
	 * @see java.lang.Throwable#getMessage()
	 */
	@Override
	public String getMessage() {
		return "Mismatch in DBR types for pv " + this.pvName  + " from source " + this.sourceDesc
				+ ". From config, we have " + this.configDBRType 
				+ ". From the source, we have " + this.streamDBRType; 
	}
	
	
}
