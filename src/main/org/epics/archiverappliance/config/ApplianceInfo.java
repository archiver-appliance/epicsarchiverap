/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import java.io.Serializable;

/**
 * Information specific to an appliance
 * 
 * @author mshankar
 *
 */
public class ApplianceInfo implements Serializable {
	private static final long serialVersionUID = -1271146670886769541L;
	private String identity;
	private String mgmtURL;
	private String engineURL;
	private String retrievalURL;
	private String etlURL;
	/**
	 * Separating out the data retrieval URL that is used by clients (for example, ArchiverViewer) to get archiver data from the system allows us to have load balancers in front of the cluster.
	 * This is not used for internal business logic and is principally used in data retrieval contexts.  
	 */
	private String dataRetrievalURL;
	private String clusterInetPort;
	
	public ApplianceInfo(String identity, String mgmtURL, String engineURL, String retrievalURL, String etlURL, String clusterInetPort, String dataRetrievalURL) {
		super();
		this.identity = identity;
		this.mgmtURL = mgmtURL;
		this.engineURL = engineURL;
		this.retrievalURL = retrievalURL;
		this.etlURL = etlURL;
		this.clusterInetPort = clusterInetPort;
		this.dataRetrievalURL = dataRetrievalURL;
	}

	/**
	 * Get the management URL for this appliance
	 * @return mgmtURL The management URL
	 */
	public String getMgmtURL() {
		return mgmtURL;
	}

	/**
	 * Get the engine URL for this appliance
	 * @return engineURL The engine URL
	 */
	public String getEngineURL() {
		return engineURL;
	}
	
	/**
	 * Get the retrieval URL for this appliance
	 * @return retrievalURL The retrieval URL
	 */
	public String getRetrievalURL() {
		return retrievalURL;
	}
	
	/**
	 * Get the ETL URL for this appliance
	 * @return etlURL The ETL URL
	 */
	public String getEtlURL() {
		return etlURL;
	}

	/**
	 * The identity of this appliance.
	 * @return identify The appliance identity
	 */
	public String getIdentity() {
		return identity;
	}

	@Override
	public boolean equals(Object obj) {
		ApplianceInfo other = (ApplianceInfo) obj;
		return this.identity.equals(other.identity);
	}

	@Override
	public int hashCode() {
		return identity.hashCode();
	}

	/**
	 * Returns the IP:port that is used for clustering.
	 * For example, this could be localhost:16670 or archiver:17670 etc
	 * @return clusterInetPort The IP:port for cluster
	 */
	public String getClusterInetPort() {
		return clusterInetPort;
	}

	public String getDataRetrievalURL() {
		return dataRetrievalURL;
	}
}
