package org.epics.archiverappliance.retrieval;

import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;

/**
 * <p>
 * This class should be used to store the PV name and type info of data that will be
 * retrieved from neighbouring nodes in a cluster, to be returned in a response from
 * the source cluster.
 * </p>
 * <p>
 * PVTypeInfo maintains a PV name field, too. At first the PV name field in this object
 * seems superfluous. But the field is necessary, as it contains the unprocessed PV
 * name as opposed to the PV name stored by the PVTypeInfo object, which has been
 * processed.
 * </p>
 * 
 * @author Michael Kenning
 *
 */
public class PVInfoForClusterRetrieval {
	
	private String pvName;
	private PVTypeInfo typeInfo;
	private PostProcessor postProcessor;
	private ApplianceInfo applianceInfo;
	
	public PVInfoForClusterRetrieval(String pvName, PVTypeInfo typeInfo, 
			PostProcessor postProcessor, ApplianceInfo applianceInfo) {
		this.pvName = pvName;
		this.typeInfo = typeInfo;
		this.postProcessor = postProcessor;
		this.applianceInfo = applianceInfo;
		
		assert(this.pvName == null);
		assert(this.typeInfo == null);
		assert(this.postProcessor == null);
		assert(this.applianceInfo == null);
	}
	
	public String getPVName() {
		return pvName;
	}
	
	public PVTypeInfo getTypeInfo() {
		return typeInfo;
	}
	
	public PostProcessor getPostProcessor() {
		return postProcessor;
	}
	
	public ApplianceInfo getApplianceInfo() {
		return applianceInfo;
	}

}
