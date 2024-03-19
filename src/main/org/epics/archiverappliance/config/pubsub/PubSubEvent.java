package org.epics.archiverappliance.config.pubsub;

import java.io.Serializable;
import java.io.StringWriter;

/**
 * Generic POJO for pubsub events within this cluster..
 * @author mshankar
 *
 */
public class PubSubEvent implements Serializable {
	private static final long serialVersionUID = -6207525235955683972L;

	/**
	 * Type of this event.
	 */
	private String type;
	
	/**
	 * Appliance identity of the appliance that originated this event. 
	 */
	private String source;
	
	/**
	 * Appliance identity of the appliance that is supposed to process this event. 
	 */
	private String destination;
	
	/**
	 * Almost all events in this application apply to a PV
	 */
	private String pvName;
	
	/**
	 * Event specific data in JSON. 
	 */
	private String eventData;
	
	private transient boolean isSourceCluster = false;
	
	public PubSubEvent(String type, String destination, String pvName) {
		super();
		this.type = type;
		this.destination = destination;
		this.pvName = pvName;
	}

	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getDestination() {
		return destination;
	}
	public void setDestination(String destination) {
		this.destination = destination;
	}
	public String getEventData() {
		return eventData;
	}
	public void setEventData(String eventData) {
		this.eventData = eventData;
	}
	
	public String generateEventDescription() { 
		StringWriter buf = new StringWriter();
		buf.append(type);
		buf.append("/");
		buf.append(source);
		buf.append("/");
		buf.append(destination);
		buf.append("/");
		buf.append(pvName);
		return buf.toString();
	}

	public boolean isSourceCluster() {
		return isSourceCluster;
	}

	public void markSourceAsCluster() {
		this.isSourceCluster = true;
	}
	
	public String getPvName() {
		return pvName;
	}

	public void setPvName(String pvName) {
		this.pvName = pvName;
	}
}
