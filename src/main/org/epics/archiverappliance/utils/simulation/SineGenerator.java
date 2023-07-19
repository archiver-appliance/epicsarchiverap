/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;

import java.util.Collections;

/**
 * Generates a sine wave with a default period of one hour and a specifiable offset.
 * Used for data generation in the unit tests.
 * 
 * @author mshankar
 *
 */
public class SineGenerator implements SimulationValueGenerator {
	private int phasediffindegress = 0;
	// By default, we let the simulation stream (or other containing stream) choose the number of samples it wants.

	public SineGenerator(int phasediffindegress) {
		this.phasediffindegress = phasediffindegress;
	}

	private static final int NUM_COPIES = 10;
	@Override
	public SampleValue getSampleValue(ArchDBRTypes type, int secondsIntoYear) {
		// We want to fit 360 degrees into an hour (3600 seconds) so each second is 1/10th of a degree. 
		double indegrees = secondsIntoYear/10.0 + phasediffindegress;
		double inradians = indegrees*Math.PI/180.0;
		double sineval = Math.sin(inradians);
		return switch (type) {
			case DBR_SCALAR_STRING, DBR_V4_GENERIC_BYTES -> new ScalarStringSampleValue(Double.toString(sineval));
			case DBR_SCALAR_SHORT, DBR_SCALAR_ENUM -> new ScalarValue<Short>((short) (Short.MAX_VALUE * sineval));
			case DBR_SCALAR_FLOAT -> new ScalarValue<Float>((float) sineval);
			case DBR_SCALAR_BYTE -> new ScalarValue<Byte>((byte) (Byte.MAX_VALUE * sineval));
			case DBR_SCALAR_INT -> new ScalarValue<Integer>((int) (Integer.MAX_VALUE * sineval));
			case DBR_SCALAR_DOUBLE -> new ScalarValue<Double>(sineval);
			case DBR_WAVEFORM_STRING ->
					new VectorStringSampleValue(Collections.nCopies(NUM_COPIES, Double.toString(sineval)));
			case DBR_WAVEFORM_SHORT, DBR_WAVEFORM_ENUM ->
					new VectorValue<Short>(Collections.nCopies(NUM_COPIES, (short) (Short.MAX_VALUE * sineval)));
			case DBR_WAVEFORM_FLOAT -> new VectorValue<Float>(Collections.nCopies(NUM_COPIES, (float) sineval));
			case DBR_WAVEFORM_BYTE ->
					new VectorValue<Byte>(Collections.nCopies(NUM_COPIES, (byte) (Byte.MAX_VALUE * sineval)));
			case DBR_WAVEFORM_INT ->
					new VectorValue<Integer>(Collections.nCopies(NUM_COPIES, (int) (Integer.MAX_VALUE * sineval)));
			case DBR_WAVEFORM_DOUBLE -> new VectorValue<Double>(Collections.nCopies(NUM_COPIES, sineval));
		};
	}

}
