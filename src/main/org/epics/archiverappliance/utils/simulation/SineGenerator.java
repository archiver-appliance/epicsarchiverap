/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import java.util.Collections;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;

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
	private int numberofsamples = -1;
	
	public SineGenerator(int phasediffindegress) {
		this.phasediffindegress = phasediffindegress;
	}
	
	public SineGenerator(int phasediffindegress, int numberofsamples) {
		this.phasediffindegress = phasediffindegress;
		this.numberofsamples = numberofsamples;
	}


	@Override
	public int getNumberOfSamples(ArchDBRTypes type) {
		return numberofsamples;
	}

	private static final int NUM_COPIES = 10;
	@Override
	public SampleValue getSampleValue(ArchDBRTypes type, int secondsIntoYear) {
		// We want to fit 360 degrees into an hour (3600 seconds) so each second is 1/10th of a degree. 
		double indegrees = secondsIntoYear/10.0 + phasediffindegress;
		double inradians = indegrees*Math.PI/180.0;
		double sineval = Math.sin(inradians);
		switch(type) {
		case DBR_SCALAR_STRING:
			return new ScalarStringSampleValue(Double.toString(sineval));
		case DBR_SCALAR_SHORT:
			return new ScalarValue<Short>((short) (Short.MAX_VALUE*sineval));
		case DBR_SCALAR_FLOAT:
			return new ScalarValue<Float>((float) sineval);
		case DBR_SCALAR_ENUM:
			return new ScalarValue<Short>((short) (Short.MAX_VALUE*sineval));
		case DBR_SCALAR_BYTE:
			return new ScalarValue<Byte>((byte) (Byte.MAX_VALUE*sineval));			
		case DBR_SCALAR_INT:
			return new ScalarValue<Integer>((int) (Integer.MAX_VALUE*sineval));
		case DBR_SCALAR_DOUBLE:
			return new ScalarValue<Double>(sineval);
		case DBR_WAVEFORM_STRING:
			return new VectorStringSampleValue(Collections.nCopies(NUM_COPIES, Double.toString(sineval)));
		case DBR_WAVEFORM_SHORT:
			return new VectorValue<Short>(Collections.nCopies(NUM_COPIES,(short) (Short.MAX_VALUE*sineval)));
		case DBR_WAVEFORM_FLOAT:
			return new VectorValue<Float>(Collections.nCopies(NUM_COPIES,(float) sineval));
		case DBR_WAVEFORM_ENUM:
			return new VectorValue<Short>(Collections.nCopies(NUM_COPIES,(short) (Short.MAX_VALUE*sineval)));
		case DBR_WAVEFORM_BYTE:
			return new VectorValue<Byte>(Collections.nCopies(NUM_COPIES,(byte) (Byte.MAX_VALUE*sineval)));			
		case DBR_WAVEFORM_INT:
			return new VectorValue<Integer>(Collections.nCopies(NUM_COPIES, (int) (Integer.MAX_VALUE*sineval)));
		case DBR_WAVEFORM_DOUBLE:
			return new VectorValue<Double>(Collections.nCopies(NUM_COPIES, sineval));
		case DBR_V4_GENERIC_BYTES:
			return new ScalarStringSampleValue(Double.toString(sineval));
		default:
			throw new UnsupportedOperationException("The sine generator does not support " + type.name());
		}
	}

}
