package org.epics.archiverappliance.utils.ui;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Generate something that converts a POJO into JSON using bean introspection.
 * Underlying framework is still json-simple.
 * This has a giant switch statement that does things based on types; so please add unit tests as part of adding new fields to objects that use JSONEncoder. 
 * @author mshankar
 *
 */
public class JSONEncoder<T> {
	private static Logger logger = LogManager.getLogger(JSONEncoder.class.getName());
	public static <T> JSONEncoder<T> getEncoder(Class<T> clazz) throws IntrospectionException {
		return new JSONEncoder<T>(clazz);
	}
	
	private LinkedList<AttributeEncoder> encoders = new LinkedList<AttributeEncoder>();
	private JSONEncoder(Class<T> clazz) throws IntrospectionException {
		BeanInfo info = Introspector.getBeanInfo(clazz);
		PropertyDescriptor[] descriptors = info.getPropertyDescriptors();
		for(PropertyDescriptor descriptor : descriptors) {
			if(descriptor.getPropertyType().equals(String.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(boolean.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(int.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(long.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(float.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(double.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(Double.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(Long.class)) {
				encoders.add(new ToStringEncoder(descriptor));
            } else if (descriptor.getPropertyType().equals(Instant.class)) {
				encoders.add(new ISO8601Encoder(descriptor));
			} else if(descriptor.getPropertyType().equals(ArchDBRTypes.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(SamplingMethod.class)) {
				encoders.add(new ToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(String[].class)) {
				encoders.add(new ArrayOfToStringEncoder(descriptor));
			} else if(descriptor.getPropertyType().equals(HashMap.class)) {
				encoders.add(new HashMapEncoder(descriptor));
			} else if(descriptor.getName().equals("class")) {
				// Skip class...
			} else {
				throw new IntrospectionException("Do not have JSON encoder for property " + descriptor.getName() + " of type " + descriptor.getPropertyType().getCanonicalName());
			}
		}
	}
	
	private static interface AttributeEncoder {
		String getProperty();
		void encode(Object obj, JSONObject jsonObj) throws IllegalAccessException, InvocationTargetException ;
	}
	
	public JSONObject encode(T obj)  throws IllegalAccessException, InvocationTargetException {
		JSONObject jsonObj = new JSONObject();
		for(AttributeEncoder encoder : encoders) {
			try {
				encoder.encode(obj, jsonObj);
			} catch(Exception ex) {
				logger.error("Exception marshalling attribute " + encoder.getProperty(), ex);
			}
		}
		return jsonObj;
	}
	
	/**
	 * Encode the object and add it to this array
	 * Dealing with JSON generates a lot of suppress warnings from raw types..
	 * @param obj T
	 * @param arrayOfObjs JSONArray
	 * @throws IllegalAccessException  &emsp; 
	 * @throws InvocationTargetException  &emsp; 
	 */
	@SuppressWarnings("unchecked")
	public void encodeAndAdd(T obj, JSONArray arrayOfObjs) throws IllegalAccessException, InvocationTargetException {
		JSONObject jsonObj = this.encode(obj);
		arrayOfObjs.add(jsonObj);
	}
	
	public void encodeAndPrint(T obj, PrintWriter out) throws IllegalAccessException, InvocationTargetException {
		JSONObject jsonObj = this.encode(obj);
		out.print(JSONValue.toJSONString(jsonObj));
	}

	
	private static class ToStringEncoder implements AttributeEncoder {
		private String propertyName;
		private Method readMethod;
		private ToStringEncoder(PropertyDescriptor descriptor) {
			propertyName = descriptor.getName();
			readMethod = descriptor.getReadMethod();
		}

		/* (non-Javadoc)
		 * @see org.epics.archiverappliance.utils.ui.JSONEncoder.AttributeEncoder#encode(java.lang.Object, org.json.simple.JSONObject)
		 * Dealing with JSON generates a lot of suppress warnings from raw types..
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void encode(Object obj, JSONObject jsonObj) throws IllegalAccessException, InvocationTargetException {
			Object val = readMethod.invoke(obj);
			if(val != null) {
				jsonObj.put(propertyName, val.toString());
			}
		}

		@Override
		public String getProperty() {
			return propertyName;
		}
	}
	
	private static class ArrayOfToStringEncoder implements AttributeEncoder { 
		private String propertyName;
		private Method readMethod;
		
		private ArrayOfToStringEncoder(PropertyDescriptor descriptor) {
			propertyName = descriptor.getName();
			readMethod = descriptor.getReadMethod();
		}

		/* (non-Javadoc)
		 * @see org.epics.archiverappliance.utils.ui.JSONEncoder.AttributeEncoder#encode(java.lang.Object, org.json.simple.JSONObject)
		 * Dealing with JSON generates a lot of suppress warnings from raw types..
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void encode(Object obj, JSONObject jsonObj) throws IllegalAccessException, InvocationTargetException {
			Object[] vals = (Object[]) readMethod.invoke(obj);
			if(vals != null) {
				JSONArray valarray = new JSONArray();
				for(Object val : vals) {
					valarray.add(val.toString());
				}
				jsonObj.put(propertyName, valarray);
			}
		}
		
		@Override
		public String getProperty() {
			return propertyName;
		}
	}
	
	private static class ISO8601Encoder implements AttributeEncoder { 
		private String propertyName;
		private Method readMethod;
		private ISO8601Encoder(PropertyDescriptor descriptor) {
			propertyName = descriptor.getName();
			readMethod = descriptor.getReadMethod();
		}

		/* (non-Javadoc)
		 * @see org.epics.archiverappliance.utils.ui.JSONEncoder.AttributeEncoder#encode(java.lang.Object, org.json.simple.JSONObject)
		 * Dealing with JSON generates a lot of suppress warnings from raw types..
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void encode(Object obj, JSONObject jsonObj) throws IllegalAccessException, InvocationTargetException {
            Instant ts = (Instant) readMethod.invoke(obj);
			if(ts != null) {
				jsonObj.put(propertyName, TimeUtils.convertToISO8601String(ts));
			}
		}
		
		@Override
		public String getProperty() {
			return propertyName;
		}
	}
	
	
	private static class HashMapEncoder implements AttributeEncoder { 
		private String propertyName;
		private Method readMethod;
		
		private HashMapEncoder(PropertyDescriptor descriptor) {
			propertyName = descriptor.getName();
			readMethod = descriptor.getReadMethod();
		}

		/* (non-Javadoc)
		 * @see org.epics.archiverappliance.utils.ui.JSONEncoder.AttributeEncoder#encode(java.lang.Object, org.json.simple.JSONObject)
		 * Dealing with JSON generates a lot of suppress warnings from raw types..
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void encode(Object obj, JSONObject jsonObj) throws IllegalAccessException, InvocationTargetException {
			HashMap<String, Object> childMap = (HashMap<String, Object>) readMethod.invoke(obj);
			if(childMap != null) {
				JSONObject childObj = new JSONObject();
				for(String key : childMap.keySet()) {
					childObj.put(key, childMap.get(key).toString());
				}
				jsonObj.put(propertyName, childObj);
			}
		}
		
		@Override
		public String getProperty() {
			return propertyName;
		}
	}

}
