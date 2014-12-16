package org.epics.archiverappliance.mgmt.policy;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

/**
 * Given the information computed by the engine about the PV, compute the archiving policy using policies.py
 * Information to the policies.py is passed in as a dictionary with these keys
 * <ol>
 * <li><code>dbrtype</code> -- The ArchDBRType of the PV</li>
 * <li><code>eventRate</code> -- The sampled event rate in events per second.</li>
 * <li><code>storageRate</code> -- The sampled storage in bytes per seconds.</li>
 * <li><code>aliasName</code> -- The value of the .NAME field for aliases</li>
 * <li><code>policyName</code> -- If the user has overridden the policy when requesting archiving, this is the name of the policy</li>
 * All the {@link ConfigService#getExtraFields() extra fields} are use the fieldName as the key (for example, ADEL comes in as .ADEL). 
 * </ol>
 * The result of policy execution is a another dictionary with these keys
 * <ol>
 * <li><code>samplingPeriod</code> -- The sampling period to use for this PV.</li>
 * <li><code>samplingMethod</code> -- The {@link SamplingMethod sampling method} to use for this PV.</li>
 * <li><code>policyName</code> -- The name of the policy that was used for this PV.</li>
 * <li><code>dataStores</code> -- An array of StoragePlugin URL's that can be parsed by {@link StoragePluginURLParser StoragePluginURLParser}. These form the stages of data storage for this PV.</li>
 * <li><code>archiveFields</code> -- A optional array of fields that will be archived as part of archiving the .VAL field for this PV.</li>
 * </ol>
 * @author mshankar
 *
 */
public class ExecutePolicy {
	private static Logger logger = Logger.getLogger(ExecutePolicy.class.getName());
	/**
	 * @param is
	 * @param pvName
	 * @param pvInfo
	 * @return
	 * @throws IOException
	 */
	public static PolicyConfig computePolicyForPV(InputStream is, String pvName, HashMap<String, Object> pvInfo) throws IOException {
		
		PythonInterpreter interp = new PythonInterpreter(null, new PySystemState());
		PyDictionary pvInfoDict = new PyDictionary();
		pvInfoDict.put("pvName", pvName);
		pvInfoDict.putAll(pvInfo);
		interp.set("pvInfo", pvInfoDict);
		// Load the policies.py into the interpreter.
		interp.execfile(is);
		interp.exec("pvPolicy = determinePolicy(pvInfo)");
		PyDictionary policy = (PyDictionary) interp.get("pvPolicy");
		PolicyConfig policyConfig = new PolicyConfig();
		Double samplingPeriod = (Double) policy.get("samplingPeriod");
		policyConfig.setSamplingPeriod(samplingPeriod.floatValue());
		String samplingMethod = (String) policy.get("samplingMethod");
		policyConfig.setSamplingMethod(SamplingMethod.valueOf(samplingMethod));
		String policyName = (String) policy.get("policyName");
		policyConfig.setPolicyName(policyName);
		LinkedList<String> dataStores = new LinkedList<String>(); 
		for(Object dataStore : (PyList) policy.get("dataStores")) {
			dataStores.add((String)dataStore);
		}
		policyConfig.setDataStores(dataStores.toArray(new String[0]));
		
		
		LinkedList<String> archiveFields = new LinkedList<String>(); 
		if(policy.containsKey("archiveFields")) {
			for(Object archiveField : (PyList) policy.get("archiveFields")) {
				archiveFields.add((String)archiveField);
			}
		} else {
			logger.debug("No additional fields will be archived for PV " + pvName);
		}
		policyConfig.setArchiveFields(archiveFields.toArray(new String[0]));
		
		if(logger.isDebugEnabled()) logger.debug("For pv" + pvName + "using policy " + policyConfig.generateStringRepresentation());
		
		interp.cleanup();
		
		return policyConfig;
	}
	
	public static HashMap<String, String> getPolicyList(InputStream is) {
		logger.debug("Getting the list of policies.");
		PythonInterpreter interp = new PythonInterpreter(null, new PySystemState());
		// Load the policies.py into the interpreter.
		interp.execfile(is);
		interp.exec("pvPolicies = getPolicyList()");
		PyDictionary policies = (PyDictionary) interp.get("pvPolicies");
		@SuppressWarnings("unchecked")
		HashMap<String, String> ret = new HashMap<String, String>(policies);
		interp.cleanup();
		return ret;
	}

	public static List<String> getFieldsArchivedAsPartOfStream(InputStream is) {
		logger.debug("Getting the list of standard fields.");
		PythonInterpreter interp = new PythonInterpreter(null, new PySystemState());
		// Load the policies.py into the interpreter.
		interp.execfile(is);
		interp.exec("pvStandardFields = getFieldsArchivedAsPartOfStream()");
		PyList stdFields = (PyList) interp.get("pvStandardFields");
		@SuppressWarnings("unchecked")
		LinkedList<String> ret = new LinkedList<String>(stdFields);
		interp.cleanup();
		return ret;
	}
}
