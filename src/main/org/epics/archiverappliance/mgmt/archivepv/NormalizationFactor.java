package org.epics.archiverappliance.mgmt.archivepv;

public class NormalizationFactor {

	private String identify;
	private float percentage;
	
	NormalizationFactor(String identify,float percentage)
	{
		this.identify=identify;
		this.percentage=percentage;
	}

	public String getIdentify() {
		return identify;
	}

	public float getPercentage() {
		return percentage;
	}
}
