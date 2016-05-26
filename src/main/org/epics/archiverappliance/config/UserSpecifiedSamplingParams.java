package org.epics.archiverappliance.config;

import java.io.Serializable;
import java.util.HashSet;

import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;

/**
 * Various options that the user can specify for archiving a PV.
 * @author mshankar
 *
 */
public class UserSpecifiedSamplingParams implements Comparable<UserSpecifiedSamplingParams>, Serializable {
	private static final long serialVersionUID = -3909878263344947887L;
	public SamplingMethod userSpecifedsamplingMethod = SamplingMethod.MONITOR;
	public float userSpecifedSamplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
	private boolean userOverrideParams = false;
	private String controllingPV = null;
	private String policyName = null;
	private String[] archiveFields = new String[0];
	private String[] aliases = new String[0];
	private boolean skipAliasCheck = false;
	private boolean usePVAccess = false;
	private boolean skipCapacityPlanning = false;
	
	
	public String getPolicyName() {
		return policyName;
	}

	public void setPolicyName(String policyName) {
		this.policyName = policyName;
	}

	public UserSpecifiedSamplingParams() {
		userOverrideParams = false;
	}

	public void setUserSpecifedsamplingMethod(
			SamplingMethod userSpecifedsamplingMethod) {
		this.userSpecifedsamplingMethod = userSpecifedsamplingMethod;
	}

	public void setUserSpecifedSamplingPeriod(float userSpecifedSamplingPeriod) {
		this.userSpecifedSamplingPeriod = userSpecifedSamplingPeriod;
	}

	public void setUserOverrideParams(boolean userOverrideParams) {
		this.userOverrideParams = userOverrideParams;
	}

	public void setControllingPV(String controllingPV) {
		this.controllingPV = controllingPV;
	}

	public UserSpecifiedSamplingParams(SamplingMethod userSpecifedsamplingMethod, float userSpecifedSamplingPeriod, String controllingPV, String policyName, boolean skipCapacityPlanning, boolean usePVAccess) {
		this.userSpecifedsamplingMethod = userSpecifedsamplingMethod;
		this.userSpecifedSamplingPeriod = userSpecifedSamplingPeriod;
		this.controllingPV = controllingPV;
		this.policyName = policyName;
		userOverrideParams = true;
		this.skipCapacityPlanning = skipCapacityPlanning;
		this.usePVAccess = usePVAccess;
	}

	@Override
	public int compareTo(UserSpecifiedSamplingParams other) {
		if(this.userSpecifedsamplingMethod == other.userSpecifedsamplingMethod) {
			if(this.userSpecifedSamplingPeriod < other.userSpecifedSamplingPeriod) {
				return -1;
			} else if(this.userSpecifedSamplingPeriod > other.userSpecifedSamplingPeriod) { 
				return 1;
			} else {
				return 0;
			}
		} else {
			return this.userSpecifedsamplingMethod.compareTo(other.userSpecifedsamplingMethod);
		}
	}

	public SamplingMethod getUserSpecifedsamplingMethod() {
		return userSpecifedsamplingMethod;
	}

	public float getUserSpecifedSamplingPeriod() {
		return userSpecifedSamplingPeriod;
	}

	public boolean isUserOverrideParams() {
		return userOverrideParams;
	}

	public String getControllingPV() {
		return controllingPV;
	}
	
	public String[] getArchiveFields() {
		return archiveFields;
	}
	
	public void setArchiveFields(String[] archiveFields) {
		if(archiveFields == null || archiveFields.length == 0) { 
			this.archiveFields = new String[0];
			return;
		}
		
		HashSet<String> newFields = new HashSet<String>();
		for(String fieldName : archiveFields) {
			if(fieldName == null || fieldName.equals("")) continue; 
			if(fieldName.equals("VAL")) continue;
			newFields.add(fieldName);
		}
		
		this.archiveFields = newFields.toArray(new String[0]);
	}
	
	
	public void addArchiveField(String fieldName) {
		if(fieldName == null || fieldName.equals("")) return; 
		if(fieldName.equals("VAL")) return;

		HashSet<String> newFields = new HashSet<String>();
		if(this.archiveFields != null) { 
			for(String fieldBeingArchived : this.archiveFields) {
				newFields.add(fieldBeingArchived);
			}
		}
		newFields.add(fieldName);
		this.archiveFields = newFields.toArray(new String[0]);
		
	}
	
	public boolean checkIfFieldAlreadySepcified(String fieldName) {
		if(fieldName == null || fieldName.equals("")) return false; 
		if(fieldName.equals("VAL")) return true;

		if(this.archiveFields != null) { 
			for(String fieldBeingArchived : this.archiveFields) {
				if(fieldBeingArchived.equals(fieldName)) {
					return true;
				}
			}
		}
		return false;
	}
	
	public boolean wereAnyFieldsSpecified() {
		return this.archiveFields != null && this.archiveFields.length > 0;
	}
	
	public void addAlias(String aliasName) { 
		if(aliasName == null || aliasName.equals("")) return;
		
		HashSet<String> newAliases = new HashSet<String>();
		if(this.aliases != null) { 
			for(String alias : this.aliases) {
				newAliases.add(alias);
			}
		}
		newAliases.add(aliasName);
		this.aliases = newAliases.toArray(new String[0]);		
	}
	
	public String[] getAliases() { 
		return aliases;
	}

	public void setAliases(String[] aliases) {
		this.aliases = aliases;
	}

	/**
	 * @return Should we skip checking .NAME to see if this PV is an alias
	 * Useful if you have a pCAS server that overloads the .NAME field for something else.
	 */
	public boolean isSkipAliasCheck() {
		return skipAliasCheck;
	}

	/**
	 * @param skipAliasCheck the skipAliasCheck to set
	 */
	public void setSkipAliasCheck(boolean skipAliasCheck) {
		this.skipAliasCheck = skipAliasCheck;
	}

	public boolean isUsePVAccess() {
		return usePVAccess;
	}

	public void setUsePVAccess(boolean usePVAccess) {
		this.usePVAccess = usePVAccess;
	}

	public boolean isSkipCapacityPlanning() {
		return skipCapacityPlanning;
	}

	public void setSkipCapacityPlanning(boolean skipCapacityPlanning) {
		this.skipCapacityPlanning = skipCapacityPlanning;
	}
}