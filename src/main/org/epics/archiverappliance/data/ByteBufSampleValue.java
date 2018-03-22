package org.epics.archiverappliance.data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.epics.pvaccess.impl.remote.IntrospectionRegistry;
import org.epics.pvaccess.impl.remote.SerializationHelper;
import org.epics.pvdata.pv.BooleanArrayData;
import org.epics.pvdata.pv.ByteArrayData;
import org.epics.pvdata.pv.DeserializableControl;
import org.epics.pvdata.pv.DoubleArrayData;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.FloatArrayData;
import org.epics.pvdata.pv.IntArrayData;
import org.epics.pvdata.pv.LongArrayData;
import org.epics.pvdata.pv.PVBoolean;
import org.epics.pvdata.pv.PVBooleanArray;
import org.epics.pvdata.pv.PVByte;
import org.epics.pvdata.pv.PVByteArray;
import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVDoubleArray;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVFloat;
import org.epics.pvdata.pv.PVFloatArray;
import org.epics.pvdata.pv.PVInt;
import org.epics.pvdata.pv.PVIntArray;
import org.epics.pvdata.pv.PVLong;
import org.epics.pvdata.pv.PVLongArray;
import org.epics.pvdata.pv.PVScalar;
import org.epics.pvdata.pv.PVScalarArray;
import org.epics.pvdata.pv.PVShort;
import org.epics.pvdata.pv.PVShortArray;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVUByte;
import org.epics.pvdata.pv.PVUByteArray;
import org.epics.pvdata.pv.PVUInt;
import org.epics.pvdata.pv.PVUIntArray;
import org.epics.pvdata.pv.PVULong;
import org.epics.pvdata.pv.PVULongArray;
import org.epics.pvdata.pv.PVUShort;
import org.epics.pvdata.pv.PVUShortArray;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.ShortArrayData;
import org.epics.pvdata.pv.StringArrayData;
import org.epics.pvdata.pv.Type;
import org.json.simple.JSONValue;

/**
 * A sample value that encapsulates a byte buffer.
 * This is principally as the "value" of a EPICS V4 PVStructure that is not mapped to any of the standard DBR types.
 * @author mshankar
 *
 */
public class ByteBufSampleValue implements SampleValue {
	ByteBuffer buf;
	public ByteBufSampleValue(ByteBuffer buf) { 
		this.buf = buf;
	}

	@Override
	public ByteBuffer getValueAsBytes() {
		return buf.asReadOnlyBuffer();
	}

	@Override
	public boolean equals(Object obj) {
		ByteBufSampleValue other = (ByteBufSampleValue) obj;
		return this.buf.equals(other.getValueAsBytes());
	}

	@Override
	public int hashCode() {
		return this.buf.hashCode();
	}
	
	private void pvStructure2JSON(PVStructure pvStructure, Map<String, Object> ret) {
		for(PVField fld : pvStructure.getPVFields()) {
			String fieldName = fld.getFieldName();
			Type type = fld.getField().getType();
			switch(type) {
			case structure: {
				PVStructure childStruct = pvStructure.getStructureField(fieldName);
				assert(childStruct != null);
				Map<String, Object> childMap = new TreeMap<String, Object>();
				pvStructure2JSON(childStruct, childMap);
				ret.put(fieldName, childMap);
				continue;
			}
			case scalar: {
				PVScalar scalarField = (PVScalar) fld;
				ScalarType elementType = scalarField.getScalar().getScalarType();
				switch(elementType) {
				case pvBoolean: {
					ret.put(fieldName, ((PVBoolean)scalarField).get());
				}
				break;
				case pvByte: {
					ret.put(fieldName, ((PVByte)scalarField).get());
				}
				break;
				case pvDouble: {
					ret.put(fieldName, ((PVDouble)scalarField).get());
				}
				break;
				case pvFloat: {
					ret.put(fieldName, ((PVFloat)scalarField).get());
				}
				break;
				case pvInt: {
					ret.put(fieldName, ((PVInt)scalarField).get());
				}
				break;
				case pvLong: {
					ret.put(fieldName, ((PVLong)scalarField).get());
				}
				break;
				case pvShort: {
					ret.put(fieldName, ((PVShort)scalarField).get());
				}
				break;
				case pvString: {
					ret.put(fieldName, ((PVString)scalarField).get());
				}
				break;
				case pvUByte: {
					ret.put(fieldName, ((PVUByte)scalarField).get());
				}
				break;
				case pvUInt: {
					ret.put(fieldName, ((PVUInt)scalarField).get());
				}
				break;
				case pvULong: {
					ret.put(fieldName, ((PVULong)scalarField).get());
				}
				break;
				case pvUShort: {
					ret.put(fieldName, ((PVUShort)scalarField).get());
				}
				break;
				default:
					throw new UnsupportedOperationException("New type in PVData? " + elementType);
				}
				continue;
			}
			case scalarArray: {
				PVScalarArray arrayField = (PVScalarArray) fld;
				ScalarType elementType = arrayField.getScalarArray().getElementType();
				switch(elementType) {
				case pvBoolean: {
						PVBooleanArray pvArray = (PVBooleanArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvBoolean);
						BooleanArrayData arrayData = new BooleanArrayData();
						pvArray.get(0, pvArray.getLength(), arrayData);
						ArrayList<Boolean> vals = new ArrayList<Boolean>();
						boolean[] data = arrayData.data;
						for(boolean dataitem : data) { 
							vals.add(dataitem);
						}
						ret.put(fieldName, vals);
				}
				break;
				case pvByte: {
					PVByteArray pvArray = (PVByteArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvByte);
					ByteArrayData arrayData = new ByteArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Byte> vals = new ArrayList<Byte>();
					byte[] data = arrayData.data;
					for(byte dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvDouble: {
						PVDoubleArray pvArray = (PVDoubleArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvDouble);
						DoubleArrayData arrayData = new DoubleArrayData();
						pvArray.get(0, pvArray.getLength(), arrayData);
						ArrayList<Double> vals = new ArrayList<Double>();
						double[] data = arrayData.data;
						for(double dataitem : data) { 
							vals.add(dataitem);
						}
						ret.put(fieldName, vals);
				}
				break;
				case pvFloat: {
					PVFloatArray pvArray = (PVFloatArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvFloat);
					FloatArrayData arrayData = new FloatArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Float> vals = new ArrayList<Float>();
					float[] data = arrayData.data;
					for(float dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvInt: {
					PVIntArray pvArray = (PVIntArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvInt);
					IntArrayData arrayData = new IntArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Integer> vals = new ArrayList<Integer>();
					int[] data = arrayData.data;
					for(int dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvLong: {
					PVLongArray pvArray = (PVLongArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvLong);
					LongArrayData arrayData = new LongArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Long> vals = new ArrayList<Long>();
					long[] data = arrayData.data;
					for(long dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvShort: {
					PVShortArray pvArray = (PVShortArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvShort);
					ShortArrayData arrayData = new ShortArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Short> vals = new ArrayList<Short>();
					short[] data = arrayData.data;
					for(short dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvString: {
					PVStringArray pvArray = (PVStringArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvString);
					StringArrayData arrayData = new StringArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<String> vals = new ArrayList<String>();
					String[] data = arrayData.data;
					for(String dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvUByte: {
					PVUByteArray pvArray = (PVUByteArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvUByte);
					ByteArrayData arrayData = new ByteArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Byte> vals = new ArrayList<Byte>();
					byte[] data = arrayData.data;
					for(byte dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvUInt: {
					PVUIntArray pvArray = (PVUIntArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvUInt);
					IntArrayData arrayData = new IntArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Integer> vals = new ArrayList<Integer>();
					int[] data = arrayData.data;
					for(int dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvULong: {
					PVULongArray pvArray = (PVULongArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvULong);
					LongArrayData arrayData = new LongArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Long> vals = new ArrayList<Long>();
					long[] data = arrayData.data;
					for(long dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				case pvUShort:{
					PVUShortArray pvArray = (PVUShortArray) pvStructure.getScalarArrayField(fieldName, ScalarType.pvUShort);
					ShortArrayData arrayData = new ShortArrayData();
					pvArray.get(0, pvArray.getLength(), arrayData);
					ArrayList<Short> vals = new ArrayList<Short>();
					short[] data = arrayData.data;
					for(short dataitem : data) { 
						vals.add(dataitem);
					}
					ret.put(fieldName, vals);
				}
				break;
				default:
					throw new UnsupportedOperationException("New type in PVData? " + elementType);
				}
				continue;
			}
			case structureArray:
			case union:
			case unionArray:
			default: {
				throw new UnsupportedOperationException();
			}
			}
			
		}
	}

	@Override
	public String toJSONString() {
		Map<String, Object> ret = new TreeMap<String, Object>();
		PVStructure pvStructure = SerializationHelper.deserializeStructureFull(this.buf, new DummyDeserializationControl());
		pvStructure2JSON(pvStructure, ret);
		return JSONValue.toJSONString(ret);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List getValues() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Number getValue(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Number getValue() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getStringValue(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getElementCount() {
		return 1;
	}

	class DummyDeserializationControl implements DeserializableControl {
		protected final IntrospectionRegistry incomingIR = new IntrospectionRegistry();

		@Override
		public void alignData(int arg0) {
		}

		@Override
		public Field cachedDeserialize(ByteBuffer buffer) {
			return incomingIR.deserialize(buffer, this);
		}

		@Override
		public void ensureData(int arg0) {
		}        
	}
}