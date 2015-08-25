package org.epics.archiverappliance.engine.generic;

import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.config.ArchDBRTypes;

public class GenericChannelParams
{
	public GenericChannelParams(float samplingPeriod, SamplingMethod mode, ArchDBRTypes dbrType)
	{
		this.samplingPeriod = samplingPeriod;
		this.mode = mode;
		this.dbrType = dbrType;
	}
	
	public final float samplingPeriod;
	public final SamplingMethod mode;
	public final ArchDBRTypes dbrType;
}
