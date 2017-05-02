package org.epics.archiverappliance.data;

import java.util.HashMap;

/**
 * EPICS PVs have additional fields like HIHI, LOLO etc that occasionally change. 
 * We stick these changes into the event stream as part of the event.
 * This interface caters to that; however, everything is cast as a string.
 * For performance reasons, occasionally the engine sticks in a complete copy of the field values as they are at that point in time into the event.
 * There is a boolean to distinguish between these fieldvalues and those that represent actual changes.
 * @author mshankar
 *
 */
public interface FieldValues {
	/**
	 * Not all events have field values. Does this event have any field values?
	 * @return true or false
	 */
	public boolean hasFieldValues();
	
	/**
	 * Do the field values in this event represent an actual change?
	 * @return true or false
	 */
	public boolean isActualChange();
	
	/**
	 * Get the fields as a HashMap.
	 * If we have more than one entry with the same key, one of the entries is returned.
	 * @return The fields as a HashMap
	 */
	public HashMap<String, String> getFields();
	
	/**
	 * @param fieldName the field name
	 * @return The field value as a string
	 */
	public String getFieldValue(String fieldName);
	
	/**
	 * Mark this event as containing actual changes to field values.
	 */
	public void markAsActualChange();
	
	/**
	 * @param fieldName the field name 
	 * @param fieldValue the field value
	 */
	public void addFieldValue(String fieldName, String fieldValue);

	/**
	 * @param fieldValues the field values as HashMap
	 * @param markAsActualChange true or false
	 */
	public void setFieldValues(HashMap<String, String> fieldValues, boolean markAsActualChange);
}
