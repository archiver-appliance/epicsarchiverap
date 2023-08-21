package org.epics.archiverappliance.data;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.pva.data.PVAAnyArray;
import org.epics.pva.data.PVABool;
import org.epics.pva.data.PVABoolArray;
import org.epics.pva.data.PVAByte;
import org.epics.pva.data.PVAByteArray;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVADoubleArray;
import org.epics.pva.data.PVAFloat;
import org.epics.pva.data.PVAFloatArray;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAIntArray;
import org.epics.pva.data.PVALong;
import org.epics.pva.data.PVALongArray;
import org.epics.pva.data.PVAShort;
import org.epics.pva.data.PVAShortArray;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.PVAStructureArray;
import org.epics.pva.data.PVATypeRegistry;
import org.epics.pva.data.PVAUnion;
import org.epics.pva.data.PVAny;
import org.json.simple.JSONValue;

/**
 * A sample value that encapsulates a byte buffer.
 * This is principally as the "value" of a EPICS V4 PVStructure that is not
 * mapped to any of the standard DBR types.
 *
 * @author mshankar
 *
 */
public class ByteBufSampleValue implements SampleValue {
	private static final Logger logger = LogManager.getLogger(ByteBufSampleValue.class.getName());
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

	private static Object pvDataToMap(PVAData pvaData) {
		if (pvaData == null) return null;

		Class<? extends PVAData> type = pvaData.getClass();

		if (type == PVAny.class) {
			Map<String, Object> childMap = new TreeMap<>();
			childMap.put("any", pvDataToMap(((PVAny) pvaData).get()));
			return childMap;
		}
		if (type == PVAStructureArray.class) {
			PVAStructure[] pvaStructures = ((PVAStructureArray) pvaData).get();
			ArrayList<Object> structuresArray = new ArrayList<>();
			for (var pvaStructure: pvaStructures) {
				structuresArray.add(pvDataToMap(pvaStructure));
			}
			return structuresArray;
		}
		if (type == PVAAnyArray.class) {
			PVAny[] pvAnies = ((PVAAnyArray) pvaData).get();
			ArrayList<Object> aList = new ArrayList<>();
			for (var pvaStructure: pvAnies) {
				aList.add(pvDataToMap(pvaStructure));
			}
			return aList;
		}
		if (type == PVAUnion.class) {
			Map<String, Object> childMap = new TreeMap<>();
			childMap.put("union", pvDataToMap(((PVAUnion) pvaData).get()));
			return childMap;
		}
		if (type == PVAStructure.class) {
			PVAStructure childStruct = (PVAStructure) pvaData;
			assert (childStruct != null);
			Map<String, Object> childMap = new TreeMap<>();
			pvStructure2JSON(childStruct, childMap);
			return childMap;
		}
		if (type == PVABool.class) {
			return ((PVABool) pvaData).get();
		}
		if (type == PVAByte.class) {
			return ((PVAByte) pvaData).get();

		}
		if (type == PVADouble.class) {
			return ((PVADouble) pvaData).get();

		}
		if (type == PVAFloat.class) {
			return ((PVAFloat) pvaData).get();

		}
		if (type == PVAInt.class) {
			return ((PVAInt) pvaData).get();

		}
		if (type == PVALong.class) {
			return ((PVALong) pvaData).get();

		}
		if (type == PVAShort.class) {
			return ((PVAShort) pvaData).get();

		}
		if (type == PVAString.class) {
			return ((PVAString) pvaData).get();

		}

		if (type == PVABoolArray.class) {
			boolean[] data = ((PVABoolArray) pvaData).get();
			ArrayList<Boolean> vals = new ArrayList<>();
			for (boolean dataitem : data) {
				vals.add(dataitem);
			}
			return vals;

		}
		if (type == PVAByteArray.class) {
			byte[] data = ((PVAByteArray) pvaData).get();
			ArrayList<Byte> vals = new ArrayList<>();
			for (byte dataitem : data) {
				vals.add(dataitem);
			}
			return vals;

		}
		if (type == PVADoubleArray.class) {
			double[] data = ((PVADoubleArray) pvaData).get();
			ArrayList<Double> vals = new ArrayList<>();
			for (double dataitem : data) {
				vals.add(dataitem);
			}
			return vals;

		}
		if (type == PVAFloatArray.class) {
			float[] data = ((PVAFloatArray) pvaData).get();
			ArrayList<Float> vals = new ArrayList<>();
			for (float dataitem : data) {
				vals.add(dataitem);
			}
			return vals;

		}
		if (type == PVAIntArray.class) {
			int[] data = ((PVAIntArray) pvaData).get();
			ArrayList<Integer> vals = new ArrayList<>();
			for (int dataitem : data) {
				vals.add(dataitem);
			}
			return vals;

		}
		if (type == PVALongArray.class) {
			long[] data = ((PVALongArray) pvaData).get();
			ArrayList<Long> vals = new ArrayList<>();
			for (long dataitem : data) {
				vals.add(dataitem);
			}
			return vals;

		}
		if (type == PVAShortArray.class) {
			short[] data = ((PVAShortArray) pvaData).get();
			ArrayList<Short> vals = new ArrayList<>();
			for (short dataitem : data) {
				vals.add(dataitem);
			}
			return vals;

		}
		if (type == PVAStringArray.class) {
			String[] data = ((PVAStringArray) pvaData).get();
			ArrayList<String> vals = new ArrayList<>();
			Collections.addAll(vals, data);
			return vals;
		}
		return null;
	}

	private static void pvStructure2JSON(PVAStructure pvStructure, Map<String, Object> ret) {
		for (PVAData pvaData : pvStructure.get()) {
			String fieldName = pvaData.getName();
			ret.put(fieldName, pvDataToMap(pvaData));
		}
	}

	@Override
	public String toJSONString() {
		Map<String, Object> ret = new TreeMap<>();
		PVATypeRegistry types = new PVATypeRegistry();
		PVAStructure pvaStructure;
		try {
			pvaStructure = (PVAStructure) types.decodeType("structure", buf);
			pvaStructure.decode(types, buf);
			pvStructure2JSON(pvaStructure, ret);
		} catch (Exception e) {
			logger.error("exception in converting pvaStructure to JSON", e);
		}
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

}