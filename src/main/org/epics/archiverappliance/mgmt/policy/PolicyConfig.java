package org.epics.archiverappliance.mgmt.policy;

import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * A PolicyConfig object is typically the right hand side of a policy.
 * It contains all the various configuration entries for archiving a PV.
 * In addition, we have mechanisms for serializing to and unmarshalling from a string representation; said string representation is what is stored in the database.
 * @author mshankar
 *
 */
public class PolicyConfig {
	private static Logger logger = Logger.getLogger(PolicyConfig.class.getName());
	public enum SamplingMethod { SCAN, MONITOR, DONT_ARCHIVE };

	public static final float DEFAULT_MONITOR_SAMPLING_PERIOD = 1.0f;
	
	private SamplingMethod samplingMethod;
	private float samplingPeriod = 1.0f;
	private String[] dataStores;
	private String policyName;
	private String[] archiveFields;

	public SamplingMethod getSamplingMethod() {
		return samplingMethod;
	}

	public float getSamplingPeriod() {
		return samplingPeriod;
	}

	public String[] getDataStores() {
		return dataStores;
	}
	
	/**
	 * Parse a string (JSON) representation of this policy into this policy object
	 * @param policyString
	 * @throws Exception
	 */
	public void parsePolicyRepresentation(String policyString) {
		// Lots of assumptions about object types.
		// If at some point in time, we determine we need more rigorous code, we should convert to using a real parser.
		JSONObject parsedObj = (JSONObject) JSONValue.parse(policyString);
		this.samplingMethod = SamplingMethod.valueOf((String)parsedObj.get("samplingMethod"));
		this.samplingPeriod = Float.parseFloat((String)parsedObj.get("samplingPeriod"));
		
		JSONArray parsedStores = (JSONArray) parsedObj.get("dataStores");
		LinkedList<String> parsedStoresList = new LinkedList<String>();
		for(Object parsedStore : parsedStores) { 
			parsedStoresList.add((String)parsedStore);
		}
		dataStores = parsedStoresList.toArray(new String[0]);
		
		if(logger.isDebugEnabled()) logger.debug("Policy object initialized from string " + this.generateStringRepresentation());
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
}
