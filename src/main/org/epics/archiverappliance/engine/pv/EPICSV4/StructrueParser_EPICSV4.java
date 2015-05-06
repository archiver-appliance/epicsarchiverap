package org.epics.archiverappliance.engine.pv.EPICSV4;

import java.util.Date;

import org.epics.pvData.pv.BooleanArrayData;
import org.epics.pvData.pv.ByteArrayData;
import org.epics.pvData.pv.DoubleArrayData;
import org.epics.pvData.pv.Field;
import org.epics.pvData.pv.FloatArrayData;
import org.epics.pvData.pv.IntArrayData;
import org.epics.pvData.pv.LongArrayData;
import org.epics.pvData.pv.PVBoolean;
import org.epics.pvData.pv.PVBooleanArray;
import org.epics.pvData.pv.PVByte;
import org.epics.pvData.pv.PVByteArray;
import org.epics.pvData.pv.PVDouble;
import org.epics.pvData.pv.PVDoubleArray;
import org.epics.pvData.pv.PVField;
import org.epics.pvData.pv.PVFloat;
import org.epics.pvData.pv.PVFloatArray;
import org.epics.pvData.pv.PVInt;
import org.epics.pvData.pv.PVIntArray;
import org.epics.pvData.pv.PVLong;
import org.epics.pvData.pv.PVLongArray;
import org.epics.pvData.pv.PVShort;
import org.epics.pvData.pv.PVShortArray;
import org.epics.pvData.pv.PVString;
import org.epics.pvData.pv.PVStringArray;
import org.epics.pvData.pv.PVStructure;
import org.epics.pvData.pv.PVStructureArray;
import org.epics.pvData.pv.Scalar;
import org.epics.pvData.pv.ScalarArray;
import org.epics.pvData.pv.ShortArrayData;
import org.epics.pvData.pv.StringArrayData;
import org.epics.pvData.pv.StructureArrayData;

public class StructrueParser_EPICSV4 {

	public static void parseStructure(PVStructure structure) {

		PVField pvfields[] = structure.getPVFields();
		// System.out.println("field size:"+pvfields.length);
		for (PVField pf : pvfields) {
			// System.out.println(pf.toString());
			// pf.isImmutable();

			Field field = pf.getField();
			String name = field.getFieldName();
			// System.out.println(name);
			switch (field.getType()) {
			case scalar: {

				Scalar scalar = (Scalar) field;
				switch (scalar.getScalarType()) {
				case pvInt: {
					int value = ((PVInt) pf).get();
					// if(name.equals("timeStamp"))

					if (name.equals("secondsPastEpoch")) {
						System.out.println(name + ":"
								+ new Date((value * 1000)));
					} else
						System.out.println(name + ":" + value);
					break;
				}
				case pvDouble: {
					double value = ((PVDouble) pf).get();
					if (name.equals("secondsPastEpoch")) {
						System.out.println(name + ":"
								+ new Date(((int) value * 1000)));
					} else
						System.out.println(name + ":" + value);
					break;
				}
				case pvLong: {
					long value = ((PVLong) pf).get();
					if (name.equals("secondsPastEpoch")) {
						System.out.println(name + ":"
								+ new Date((value * 1000)));
					} else
						System.out.println(name + ":" + value);
					break;
				}
				case pvString: {
					String value = ((PVString) pf).get();
					System.out.println(name + ":" + value);
					break;
				}
				case pvBoolean: {
					boolean value = ((PVBoolean) pf).get();
					System.out.println(name + ":" + value);
					break;
				}
				case pvByte: {
					byte value = ((PVByte) pf).get();
					System.out.println(name + ":" + value);
					break;
				}
				case pvFloat: {
					float value = ((PVFloat) pf).get();
					System.out.println(name + ":" + value);
					break;
				}
				case pvShort: {
					short value = ((PVShort) pf).get();
					System.out.println(name + ":" + value);
					break;
				}

				default: {

					System.out.println(name + ":unkown type");

				}

				}
				break;
			}

			case structure: {
				System.out.println(name + ":scalarArray type");
				PVStructure tempStructure = (PVStructure) pf;
				parseStructure(tempStructure);
				break;
			}
			case scalarArray: {
				// System.out.println(name+":scalarArray type");
				ScalarArray tempScalarArray = (ScalarArray) field;
				// System.out.println(pf.toString());
				// field pf
				switch (tempScalarArray.getElementType()) {

				case pvBoolean: {
					PVBooleanArray tempPVBooleanArray = (PVBooleanArray) pf;
					BooleanArrayData boolData = new BooleanArrayData();
					tempPVBooleanArray.get(0, tempPVBooleanArray.getLength(),
							boolData);
					String totalData = name + ":";
					for (boolean tempData : boolData.data) {
						// System.out.print(tempData+",");
						totalData = totalData + tempData + ",";
					}
					System.out.println(totalData);
					break;

				}
				case pvByte: {

					PVByteArray tempPVByteArray = (PVByteArray) pf;
					ByteArrayData byteData = new ByteArrayData();
					tempPVByteArray.get(0, tempPVByteArray.getLength(),
							byteData);
					String totalData = name + ":";
					for (byte tempData : byteData.data) {
						// System.out.print(tempData+",");
						totalData = totalData + tempData + ",";
					}
					System.out.println(totalData);
					break;

				}
				case pvDouble: {
					PVDoubleArray tempPVDoubleArray = (PVDoubleArray) pf;
					DoubleArrayData doubleData = new DoubleArrayData();
					tempPVDoubleArray.get(0, tempPVDoubleArray.getLength(),
							doubleData);
					String totalData = name + ":";
					for (double tempData : doubleData.data) {
						// System.out.print(tempData+",");
						totalData = totalData + tempData + ",";
					}
					System.out.println(totalData);
					break;
				}
				case pvFloat: {
					PVFloatArray tempPVFloatArray = (PVFloatArray) pf;
					FloatArrayData floatData = new FloatArrayData();
					tempPVFloatArray.get(0, tempPVFloatArray.getLength(),
							floatData);
					String totalData = name + ":";
					for (float tempData : floatData.data) {
						// System.out.print(tempData+",");
						totalData = totalData + tempData + ",";
					}
					System.out.println(totalData);
					break;
				}
				case pvInt: {
					PVIntArray tempPVIntArray = (PVIntArray) pf;
					IntArrayData intData = new IntArrayData();
					tempPVIntArray.get(0, tempPVIntArray.getLength(), intData);
					String totalData = name + ":";
					for (int tempData : intData.data) {
						// System.out.print(tempData+",");
						totalData = totalData + tempData + ",";
					}
					System.out.println(totalData);
					break;
				}
				case pvLong: {
					PVLongArray tempPVLongArray = (PVLongArray) pf;
					LongArrayData longData = new LongArrayData();
					tempPVLongArray.get(0, tempPVLongArray.getLength(),
							longData);
					String totalData = name + ":";
					for (long tempData : longData.data) {
						// System.out.print(tempData+",");
						totalData = totalData + tempData + ",";
					}
					System.out.println(totalData);
					break;
				}
				case pvShort: {

					PVShortArray tempPVShortArray = (PVShortArray) pf;
					ShortArrayData shortData = new ShortArrayData();
					tempPVShortArray.get(0, tempPVShortArray.getLength(),
							shortData);
					String totalData = name + ":";
					for (short tempData : shortData.data) {
						// System.out.print(tempData+",");
						totalData = totalData + tempData + ",";
					}
					System.out.println(totalData);
					break;
				}
				case pvString: {
					// System.out.println(name+":");
					PVStringArray tempStringArray = (PVStringArray) pf;
					StringArrayData stringData = new StringArrayData();
					tempStringArray.get(0, tempStringArray.getLength(),
							stringData);
					// System.out.println(stringData.data.toString());
					String totalData = name + ":";
					for (String tempData : stringData.data) {
						// System.out.print(tempData+",");
						totalData = totalData + tempData + ",";
					}
					System.out.println(totalData);
					break;
				}

				default: {
					System.out.println(name + ":unknown type");
				}

				}

				break;

			}
			case structureArray: {
				System.out.println(name + ":structureArray");

				// System.out.println(name+":scalarArray type");
				// StructureArray tempStructureArray=(StructureArray)field;
				PVStructureArray pvsa = (PVStructureArray) pf;
				StructureArrayData structureData = new StructureArrayData();
				pvsa.get(0, pvsa.getLength(), structureData);

				// structureData.data;
				for (PVStructure tempStructure : structureData.data) {
					parseStructure(tempStructure);
				}

				break;
			}

			default: {

				System.out.println(name + ":unkown type");

			}
			}
		}// end for
	}
}
