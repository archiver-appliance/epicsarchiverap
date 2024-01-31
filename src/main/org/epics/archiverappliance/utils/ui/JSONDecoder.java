package org.epics.archiverappliance.utils.ui;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Generate something that marshalls JSON into a POJO using bean introspection.
 * Underlying framework is still json-simple.
 * This has a giant switch statement that does things based on types; so please add unit tests as part of adding new fields to objects that use JSONDecoder. 
 * @author mshankar
 *
 */
public class JSONDecoder<T> {
	private static Logger logger = LogManager.getLogger(JSONDecoder.class.getName());
	public static <T> JSONDecoder<T> getDecoder(Class<T> clazz) throws IntrospectionException, NoSuchMethodException {
		return new JSONDecoder<T>(clazz);
	}

	private LinkedList<AttributeDecoder<T>> decoders = new LinkedList<AttributeDecoder<T>>();

	private JSONDecoder(Class<T> clazz) throws IntrospectionException, NoSuchMethodException {
		BeanInfo info = Introspector.getBeanInfo(clazz);
		PropertyDescriptor[] descriptors = info.getPropertyDescriptors();
		for(PropertyDescriptor descriptor : descriptors) {
			logger.debug("Generating decoder for " + descriptor.getName());
			if(descriptor.getPropertyType().equals(String.class)) {
				decoders.add(new StringConstructor<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(boolean.class)) {
				decoders.add(new ValueOfDecoder<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(int.class)) {
				decoders.add(new ValueOfDecoder<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(long.class)) {
				decoders.add(new ValueOfDecoder<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(float.class)) {
				decoders.add(new ValueOfDecoder<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(double.class)) {
				decoders.add(new ValueOfDecoder<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(Long.class)) {
				decoders.add(new StringConstructor<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(Double.class)) {
				decoders.add(new StringConstructor<T>(descriptor));
            } else if (descriptor.getPropertyType().equals(Instant.class)) {
				decoders.add(new ISO8601Decoder<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(ArchDBRTypes.class)) {
				decoders.add(new EnumDecoder<T>(descriptor, ArchDBRTypes.class));
			} else if(descriptor.getPropertyType().equals(SamplingMethod.class)) {
				decoders.add(new EnumDecoder<T>(descriptor, SamplingMethod.class));
			} else if(descriptor.getPropertyType().equals(String[].class)) {
				decoders.add(new ArrayOfStringsDecoder<T>(descriptor));
			} else if(descriptor.getPropertyType().equals(HashMap.class)) {
				decoders.add(new HashMapDecoder<T>(descriptor));
			} else if(descriptor.getName().equals("class")) {
				// Skip class...
			} else {
				throw new IntrospectionException("Do not have JSON decoder for property " + descriptor.getName() + " of type " + descriptor.getPropertyType().getCanonicalName());
			}
		}
	}
	
	public void decode(JSONObject jsonObj, T obj)  throws IllegalAccessException, InvocationTargetException, InstantiationException {
		for(AttributeDecoder<T> decoder : decoders) {
			decoder.decode(jsonObj, obj);
		}
	}
	
	private static interface AttributeDecoder<T> {
		void decode(JSONObject jsonObj, T obj) throws IllegalAccessException, InvocationTargetException, InstantiationException ;
	}
	
	private static String WRITE_METHOD_EXPLANATION = ". Our custom JSON decoder requires both get and set methods in standard bean syntax. Use Eclipse to add a 'standard' set method";
	
	private static class StringConstructor<T> implements AttributeDecoder<T> {
		private String propertyName;
		private Method writeMethod;
		private Constructor<?> constructorFromString;

		private StringConstructor(PropertyDescriptor descriptor) throws NoSuchMethodException {
			propertyName = descriptor.getName();
			writeMethod = descriptor.getWriteMethod(); 
			if(writeMethod == null) throw new NoSuchMethodException("No write method for " + propertyName + WRITE_METHOD_EXPLANATION);
			constructorFromString = descriptor.getPropertyType().getConstructor(String.class);
		}

		@Override
		public void decode(JSONObject jsonObj, T obj) throws IllegalAccessException, InvocationTargetException, InstantiationException {
			if(jsonObj.containsKey(propertyName)) {
				String val = (String) jsonObj.get(propertyName);
				Object newObj = constructorFromString.newInstance(val);
				writeMethod.invoke(obj, newObj);
			}
		}
	}
	
	

	private static class ArrayOfStringsDecoder<T> implements AttributeDecoder<T> {
		private String propertyName;
		private Method writeMethod;
		private Constructor<?> constructorFromString;

		private ArrayOfStringsDecoder(PropertyDescriptor descriptor) throws NoSuchMethodException {
			propertyName = descriptor.getName();
			writeMethod = descriptor.getWriteMethod(); 
			if(writeMethod == null) throw new NoSuchMethodException("No write method for " + propertyName + WRITE_METHOD_EXPLANATION);
			constructorFromString = descriptor.getPropertyType().getComponentType().getConstructor(String.class);
		}

		@Override
		public void decode(JSONObject jsonObj, T obj) throws IllegalAccessException, InvocationTargetException, InstantiationException {
			if(jsonObj.containsKey(propertyName)) {
				JSONArray vals = (JSONArray) jsonObj.get(propertyName);
				LinkedList<String> newvals = new LinkedList<String>();
				for(Object val : vals) {
					newvals.add((String)constructorFromString.newInstance(val));
				}
				writeMethod.invoke(obj, new Object[]{newvals.toArray(new String[0])});
			}
		}
	}
	
	
	private static class HashMapDecoder<T> implements AttributeDecoder<T> {
		private String propertyName;
		private Method writeMethod;

		private HashMapDecoder(PropertyDescriptor descriptor) throws NoSuchMethodException {
			propertyName = descriptor.getName();
			writeMethod = descriptor.getWriteMethod(); 
			if(writeMethod == null) throw new NoSuchMethodException("No write method for " + propertyName + WRITE_METHOD_EXPLANATION);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void decode(JSONObject jsonObj, T obj) throws IllegalAccessException, InvocationTargetException, InstantiationException {
			if(jsonObj.containsKey(propertyName)) {
				HashMap<String, String> vals = (HashMap<String, String>) jsonObj.get(propertyName);
				writeMethod.invoke(obj, new Object[]{vals});
			}
		}
	}

	
	private static class ValueOfDecoder<T> implements AttributeDecoder<T> {
		private static Map<Class<?>,Class<?>> primitiveMap = new HashMap<Class<?>,Class<?>>();
		static {
			primitiveMap.put(boolean.class, Boolean.class);
			primitiveMap.put(byte.class, Byte.class);
			primitiveMap.put(char.class, Character.class);
			primitiveMap.put(short.class, Short.class);
			primitiveMap.put(int.class, Integer.class);
			primitiveMap.put(long.class, Long.class);
			primitiveMap.put(float.class, Float.class);
			primitiveMap.put(double.class, Double.class);
		}

		private String propertyName;
		private Method writeMethod;
		private Method valueOfMethod;

		private ValueOfDecoder(PropertyDescriptor descriptor) throws NoSuchMethodException {
			propertyName = descriptor.getName();
			writeMethod = descriptor.getWriteMethod();
			if(writeMethod == null) throw new NoSuchMethodException("No write method for " + propertyName + WRITE_METHOD_EXPLANATION);
			Class<?> wrapperClass = primitiveMap.get(descriptor.getPropertyType());
			valueOfMethod = wrapperClass.getMethod("valueOf", String.class);
		}

		@Override
		public void decode(JSONObject jsonObj, T obj) throws IllegalAccessException, InvocationTargetException, InstantiationException {
			if(jsonObj.containsKey(propertyName)) {
				String val = (String) jsonObj.get(propertyName);
				Object newObj = valueOfMethod.invoke(null, val);
				writeMethod.invoke(obj, newObj);
			}
		}
	}
	


	private static class ISO8601Decoder<T> implements AttributeDecoder<T> {
		private String propertyName;
		private Method writeMethod;

		private ISO8601Decoder(PropertyDescriptor descriptor) throws NoSuchMethodException {
			propertyName = descriptor.getName();
			writeMethod = descriptor.getWriteMethod(); 
		}

		@Override
		public void decode(JSONObject jsonObj, T obj) throws IllegalAccessException, InvocationTargetException, InstantiationException {
			if(jsonObj.containsKey(propertyName)) {
				String tsstr = (String) jsonObj.get(propertyName);
                Instant ts = TimeUtils.convertFromISO8601String(tsstr);
				writeMethod.invoke(obj, ts);
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private static class EnumDecoder<T> implements AttributeDecoder<T> {
		private String propertyName;
		private Method writeMethod;
		private Class<? extends Enum> enumClass;

		private EnumDecoder(PropertyDescriptor descriptor, Class<? extends Enum> enumClass) throws NoSuchMethodException {
			propertyName = descriptor.getName();
			writeMethod = descriptor.getWriteMethod();
			if(writeMethod == null) throw new NoSuchMethodException("No write method for " + propertyName + WRITE_METHOD_EXPLANATION);
			this.enumClass = enumClass; 
		}

		@SuppressWarnings("unchecked")
		@Override
		public void decode(JSONObject jsonObj, T obj) throws IllegalAccessException, InvocationTargetException, InstantiationException {
			if(jsonObj.containsKey(propertyName)) {
				String enumvalue = (String) jsonObj.get(propertyName);
				Enum enumVal = Enum.valueOf(enumClass, enumvalue);
				writeMethod.invoke(obj, enumVal);
			}
		}
	}


}
