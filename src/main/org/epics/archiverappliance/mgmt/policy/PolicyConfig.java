package org.epics.archiverappliance.mgmt.policy;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * A PolicyConfig object is typically the right hand side of a policy.
 * It contains all the various configuration entries for archiving a PV.
 * In addition, we have mechanisms for serializing to and unmarshalling from a string representation; said string representation is what is stored in the database.
 * @author mshankar
 *
 */
public class PolicyConfig {
	public enum SamplingMethod { SCAN, MONITOR, DONT_ARCHIVE };

	public static final float DEFAULT_MONITOR_SAMPLING_PERIOD = 1.0f;
	
	private SamplingMethod samplingMethod;
	private float samplingPeriod = 1.0f;
	private String[] dataStores;
	private String policyName;
	private String[] archiveFields;
	private String appliance;

	public SamplingMethod getSamplingMethod() {
		return samplingMethod;
	}

	public float getSamplingPeriod() {
		return samplingPeriod;
	}

	public String[] getDataStores() {
		return dataStores;
	}
	
	// We need this SuppressWarnings here as JSON is too dynamic.
	@SuppressWarnings("unchecked")
	public String generateStringRepresentation() {
		JSONObject stringRep = new JSONObject();
		stringRep.put("samplingMethod", samplingMethod.toString());
		stringRep.put("samplingPeriod", Float.toString(samplingPeriod));
		stringRep.put("policyName", policyName);
		JSONArray stores = new JSONArray();
		for(String store : dataStores) stores.add(store);
		stringRep.put("dataStores", stores);
		if(this.appliance != null) { 
			stringRep.put("appliance", appliance); 
		}
		return stringRep.toJSONString();
	}

	public PolicyConfig setSamplingMethod(SamplingMethod samplingMethod) {
		this.samplingMethod = samplingMethod;
		return this;
	}

	public PolicyConfig setSamplingPeriod(float samplingPeriod) {
		this.samplingPeriod = samplingPeriod;
		return this;
	}

	public PolicyConfig setDataStores(String[] dataStores) {
		this.dataStores = dataStores;
		return this;
	}

	public String getPolicyName() {
		return policyName;
	}

	public void setPolicyName(String policyName) {
		this.policyName = policyName;
	}

	public String[] getArchiveFields() {
		return archiveFields;
	}

	public void setArchiveFields(String[] archiveFields) {
		this.archiveFields = archiveFields;
	}

	public String getAppliance() {
		return appliance;
	}

	public void setAppliance(String appliance) {
		this.appliance = appliance;
	}
}
