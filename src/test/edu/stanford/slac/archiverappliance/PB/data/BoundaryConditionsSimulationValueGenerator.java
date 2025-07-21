/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_TIME_Byte;
import gov.aps.jca.dbr.DBR_TIME_Double;
import gov.aps.jca.dbr.DBR_TIME_Enum;
import gov.aps.jca.dbr.DBR_TIME_Float;
import gov.aps.jca.dbr.DBR_TIME_Int;
import gov.aps.jca.dbr.DBR_TIME_Short;
import gov.aps.jca.dbr.DBR_TIME_String;
import org.apache.commons.lang3.ArrayUtils;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.ByteBufSampleValue;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.archiverappliance.utils.simulation.SimulationValueGenerator;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * Generates a sample value based on secondsintoyear for each DBR_type.
 * The value generated is predictable; therefore, it can be used for unit tests.
 * @author mshankar
 *
 */
public class BoundaryConditionsSimulationValueGenerator implements SimulationValueGenerator {

    private static List getVector(ArchDBRTypes dbrType, int secondsIntoYear) {
        switch (dbrType) {
            case DBR_WAVEFORM_STRING -> {
                // Varying number of copies of a typical value
                return Collections.nCopies(secondsIntoYear % 10, Integer.toString(secondsIntoYear));
            }
            case DBR_WAVEFORM_SHORT, DBR_WAVEFORM_ENUM -> {
                return Collections.nCopies(secondsIntoYear % 10, (short) secondsIntoYear);
            }
            case DBR_WAVEFORM_FLOAT -> {
                // Varying number of copies of a typical value
                return Collections.nCopies(secondsIntoYear % 10, (float) Math.cos(secondsIntoYear * Math.PI / 3600));
            }
            case DBR_WAVEFORM_BYTE -> {
                // Large number of elements in the array
                return Collections.nCopies(secondsIntoYear % 10, ((byte) (secondsIntoYear % 255)));
            }
            case DBR_WAVEFORM_INT -> {
                // Varying number of copies of a typical value
                return Collections.nCopies(secondsIntoYear % 10, secondsIntoYear * secondsIntoYear);
            }
            case DBR_WAVEFORM_DOUBLE -> {
                // Varying number of copies of a typical value
                return Collections.nCopies(secondsIntoYear % 10, Math.sin(secondsIntoYear * Math.PI / 3600));
            }
            default -> throw new RuntimeException("We seemed to have missed a DBR type when generating sample data");
        }
    }

    /**
     * Get a value based on the DBR type.
     * We should check for boundary conditions here and make sure PB does not throw exceptions when we come close to MIN_ and MAX_ values
     * @param type
     * @param secondsIntoYear
     * @return
     */
    public SampleValue getSampleValue(ArchDBRTypes type, int secondsIntoYear) {
        switch (type) {
            case DBR_SCALAR_STRING -> {
                return new ScalarStringSampleValue(Integer.toString(secondsIntoYear));
            }
            case DBR_SCALAR_SHORT -> {
                if (0 <= secondsIntoYear && secondsIntoYear < 1000) {
                    // Check for some numbers around the minimum value
                    return new ScalarValue<Short>((short) (Short.MIN_VALUE + secondsIntoYear));
                } else if (1000 <= secondsIntoYear && secondsIntoYear < 2000) {
                    // Check for some numbers around the maximum value
                    return new ScalarValue<Short>((short) (Short.MAX_VALUE - (secondsIntoYear - 1000)));
                } else {
                    // Check for some numbers around 0
                    return new ScalarValue<Short>((short) (secondsIntoYear - 2000));
                }
            }
            case DBR_SCALAR_FLOAT -> {
                if (0 <= secondsIntoYear && secondsIntoYear < 1000) {
                    // Check for some numbers around the minimum value
                    return new ScalarValue<Float>(Float.MIN_VALUE + secondsIntoYear);
                } else if (1000 <= secondsIntoYear && secondsIntoYear < 2000) {
                    // Check for some numbers around the maximum value
                    return new ScalarValue<Float>(Float.MAX_VALUE - (secondsIntoYear - 1000));
                } else {
                    // Check for some numbers around 0. Divide by a large number to make sure we cater to the number of
                    // precision digits
                    return new ScalarValue<Float>((secondsIntoYear - 2000.0f) / secondsIntoYear);
                }
            }
            case DBR_SCALAR_ENUM -> {
                return new ScalarValue<Short>((short) secondsIntoYear);
            }
            case DBR_SCALAR_BYTE -> {
                return new ScalarValue<Byte>(((byte) (secondsIntoYear % 255)));
            }
            case DBR_SCALAR_INT -> {
                if (0 <= secondsIntoYear && secondsIntoYear < 1000) {
                    // Check for some numbers around the minimum value
                    return new ScalarValue<Integer>(Integer.MIN_VALUE + secondsIntoYear);
                } else if (1000 <= secondsIntoYear && secondsIntoYear < 2000) {
                    // Check for some numbers around the maximum value
                    return new ScalarValue<Integer>(Integer.MAX_VALUE - (secondsIntoYear - 1000));
                } else {
                    // Check for some numbers around 0
                    return new ScalarValue<Integer>(secondsIntoYear - 2000);
                }
            }
            case DBR_SCALAR_DOUBLE -> {
                if (0 <= secondsIntoYear && secondsIntoYear < 1000) {
                    // Check for some numbers around the minimum value
                    return new ScalarValue<Double>(Double.MIN_VALUE + secondsIntoYear);
                } else if (1000 <= secondsIntoYear && secondsIntoYear < 2000) {
                    // Check for some numbers around the maximum value
                    return new ScalarValue<Double>(Double.MAX_VALUE - (secondsIntoYear - 1000));
                } else {
                    // Check for some numbers around 0. Divide by a large number to make sure we cater to the number of
                    // precision digits
                    return new ScalarValue<Double>((secondsIntoYear - 2000.0) / (secondsIntoYear * 1000000));
                }
            }
            case DBR_WAVEFORM_STRING -> {
                // Varying number of copies of a typical value
                return new VectorStringSampleValue((List<String>) (getVector(type, secondsIntoYear)));
            }
            case DBR_WAVEFORM_SHORT, DBR_WAVEFORM_ENUM -> {
                return new VectorValue<Short>((List<Short>) (getVector(type, secondsIntoYear)));
            }
            case DBR_WAVEFORM_FLOAT -> {
                // Varying number of copies of a typical value
                return new VectorValue<Float>((List<Float>) (getVector(type, secondsIntoYear)));
            }
            case DBR_WAVEFORM_BYTE -> {
                // Large number of elements in the array
                return new VectorValue<Byte>((List<Byte>) (getVector(type, secondsIntoYear)));
            }
            case DBR_WAVEFORM_INT -> {
                // Varying number of copies of a typical value
                return new VectorValue<Integer>((List<Integer>) (getVector(type, secondsIntoYear)));
            }
            case DBR_WAVEFORM_DOUBLE -> {
                // Varying number of copies of a typical value
                return new VectorValue<Double>((List<Double>) (getVector(type, secondsIntoYear)));
            }
            case DBR_V4_GENERIC_BYTES -> {
                // Varying number of copies of a typical value
                ByteBuffer buf = ByteBuffer.allocate(1024 * 10);
                buf.put(Integer.toString(secondsIntoYear).getBytes());
                buf.flip();
                return new ByteBufSampleValue(buf);
            }
            default -> throw new RuntimeException("We seemed to have missed a DBR type when generating sample data");
        }
    }

    private static gov.aps.jca.dbr.TimeStamp convertSecondsIntoYear2JCATimeStamp(int secondsintoYear) {
        return new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + secondsintoYear);
    }

    public DBR getJCASampleValue(ArchDBRTypes type, int secondsIntoYear) {
        switch (type) {
            case DBR_SCALAR_STRING -> {
                DBR_TIME_String retvalss = new DBR_TIME_String(new String[] {Integer.toString(secondsIntoYear)});
                retvalss.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvalss.setSeverity(1);
                retvalss.setStatus(0);
                return retvalss;
            }
            case DBR_SCALAR_SHORT -> {
                DBR_TIME_Short retvalsh;
                if (0 <= secondsIntoYear && secondsIntoYear < 1000) {
                    // Check for some numbers around the minimum value
                    retvalsh = new DBR_TIME_Short(new short[] {(short) (Short.MIN_VALUE + secondsIntoYear)});
                } else if (1000 <= secondsIntoYear && secondsIntoYear < 2000) {
                    // Check for some numbers around the maximum value
                    retvalsh = new DBR_TIME_Short(new short[] {(short) (Short.MAX_VALUE - (secondsIntoYear - 1000))});
                } else {
                    // Check for some numbers around 0
                    retvalsh = new DBR_TIME_Short(new short[] {(short) (secondsIntoYear - 2000)});
                }
                retvalsh.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvalsh.setSeverity(1);
                retvalsh.setStatus(0);
                return retvalsh;
            }
            case DBR_SCALAR_FLOAT -> {
                DBR_TIME_Float retvalfl;
                if (0 <= secondsIntoYear && secondsIntoYear < 1000) {
                    // Check for some numbers around the minimum value
                    retvalfl = new DBR_TIME_Float(new float[] {Float.MIN_VALUE + secondsIntoYear});
                } else if (1000 <= secondsIntoYear && secondsIntoYear < 2000) {
                    // Check for some numbers around the maximum value
                    retvalfl = new DBR_TIME_Float(new float[] {Float.MAX_VALUE - (secondsIntoYear - 1000)});
                } else {
                    // Check for some numbers around 0. Divide by a large number to make sure we cater to the number of
                    // precision digits
                    retvalfl = new DBR_TIME_Float(new float[] {(secondsIntoYear - 2000.0f) / secondsIntoYear});
                }
                retvalfl.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvalfl.setSeverity(1);
                retvalfl.setStatus(0);
                return retvalfl;
            }
            case DBR_SCALAR_ENUM -> {
                DBR_TIME_Enum retvalen;
                retvalen = new DBR_TIME_Enum(new short[] {(short) (secondsIntoYear)});
                retvalen.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvalen.setSeverity(1);
                retvalen.setStatus(0);
                return retvalen;
            }
            case DBR_SCALAR_BYTE -> {
                DBR_TIME_Byte retvalby;
                retvalby = new DBR_TIME_Byte(new byte[] {((byte) (secondsIntoYear % 255))});
                retvalby.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvalby.setSeverity(1);
                retvalby.setStatus(0);
                return retvalby;
            }
            case DBR_SCALAR_INT -> {
                DBR_TIME_Int retvalint;
                if (0 <= secondsIntoYear && secondsIntoYear < 1000) {
                    // Check for some numbers around the minimum value
                    retvalint = new DBR_TIME_Int(new int[] {Integer.MIN_VALUE + secondsIntoYear});
                } else if (1000 <= secondsIntoYear && secondsIntoYear < 2000) {
                    // Check for some numbers around the maximum value
                    retvalint = new DBR_TIME_Int(new int[] {Integer.MAX_VALUE - (secondsIntoYear - 1000)});
                } else {
                    // Check for some numbers around 0
                    retvalint = new DBR_TIME_Int(new int[] {(secondsIntoYear - 2000)});
                }
                retvalint.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvalint.setSeverity(1);
                retvalint.setStatus(0);
                return retvalint;
            }
            case DBR_SCALAR_DOUBLE -> {
                DBR_TIME_Double retvaldb;
                if (0 <= secondsIntoYear && secondsIntoYear < 1000) {
                    // Check for some numbers around the minimum value
                    retvaldb = new DBR_TIME_Double(new double[] {(Double.MIN_VALUE + secondsIntoYear)});
                } else if (1000 <= secondsIntoYear && secondsIntoYear < 2000) {
                    // Check for some numbers around the maximum value
                    retvaldb = new DBR_TIME_Double(new double[] {(Double.MAX_VALUE - (secondsIntoYear - 1000))});
                } else {
                    // Check for some numbers around 0. Divide by a large number to make sure we cater to the number of
                    // precision digits
                    retvaldb = new DBR_TIME_Double(
                            new double[] {((secondsIntoYear - 2000.0) / (secondsIntoYear * 1000000))});
                }
                retvaldb.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvaldb.setSeverity(1);
                retvaldb.setStatus(0);
                return retvaldb;
            }
            case DBR_WAVEFORM_STRING -> {
                DBR_TIME_String retvst;
                // Varying number of copies of a typical value
                retvst =
                        new DBR_TIME_String(((List<String>) (getVector(type, secondsIntoYear))).toArray(new String[0]));
                retvst.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvst.setSeverity(1);
                retvst.setStatus(0);
                return retvst;
            }
            case DBR_WAVEFORM_SHORT -> {
                DBR_TIME_Short retvsh;
                retvsh = new DBR_TIME_Short(ArrayUtils.toPrimitive(
                        ((List<Short>) (getVector(type, secondsIntoYear))).toArray(new Short[0])));
                retvsh.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvsh.setSeverity(1);
                retvsh.setStatus(0);
                return retvsh;
            }
            case DBR_WAVEFORM_FLOAT -> {
                DBR_TIME_Float retvf;
                // Varying number of copies of a typical value
                retvf = new DBR_TIME_Float(ArrayUtils.toPrimitive(
                        ((List<Float>) (getVector(type, secondsIntoYear))).toArray(new Float[0])));
                retvf.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvf.setSeverity(1);
                retvf.setStatus(0);
                return retvf;
            }
            case DBR_WAVEFORM_ENUM -> {
                DBR_TIME_Enum retven;
                retven = new DBR_TIME_Enum(ArrayUtils.toPrimitive(
                        ((List<Short>) (getVector(type, secondsIntoYear))).toArray(new Short[0])));
                retven.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retven.setSeverity(1);
                retven.setStatus(0);
                return retven;
            }
            case DBR_WAVEFORM_BYTE -> {
                DBR_TIME_Byte retvb;
                // Large number of elements in the array
                retvb = new DBR_TIME_Byte(
                        ArrayUtils.toPrimitive(((List<Byte>) (getVector(type, secondsIntoYear))).toArray(new Byte[0])));
                retvb.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvb.setSeverity(1);
                retvb.setStatus(0);
                return retvb;
            }
            case DBR_WAVEFORM_INT -> {
                DBR_TIME_Int retvint;
                // Varying number of copies of a typical value
                retvint = new DBR_TIME_Int(ArrayUtils.toPrimitive(
                        ((List<Integer>) (getVector(type, secondsIntoYear))).toArray(new Integer[0])));
                retvint.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvint.setSeverity(1);
                retvint.setStatus(0);
                return retvint;
            }
            case DBR_WAVEFORM_DOUBLE -> {
                DBR_TIME_Double retvd;
                // Varying number of copies of a typical value
                retvd = new DBR_TIME_Double(ArrayUtils.toPrimitive(
                        ((List<Double>) (getVector(type, secondsIntoYear))).toArray(new Double[0])));
                retvd.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(secondsIntoYear));
                retvd.setSeverity(1);
                retvd.setStatus(0);
                return retvd;
            }
            case DBR_V4_GENERIC_BYTES -> throw new RuntimeException(
                    "Currently don't support " + type + " when generating sample data");
            default -> throw new RuntimeException("We seemed to have missed a DBR type when generating sample data");
        }
    }
}
