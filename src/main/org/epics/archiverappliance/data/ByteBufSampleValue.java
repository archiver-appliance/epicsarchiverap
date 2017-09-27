package org.epics.archiverappliance.data;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.epics.pvaccess.impl.remote.IntrospectionRegistry;
import org.epics.pvaccess.impl.remote.SerializationHelper;
import org.epics.pvdata.pv.DeserializableControl;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVStructure;
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
			case scalar: { 
				ret.put(fieldName, fld.toString());
				continue;
			}
			case structure: {
				PVStructure childStruct = pvStructure.getStructureField(fieldName);
				assert(childStruct != null);
				Map<String, Object> childMap = new TreeMap<String, Object>();
				pvStructure2JSON(childStruct, childMap);
				ret.put(fieldName, childMap);
				continue;
			}
			case scalarArray:
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