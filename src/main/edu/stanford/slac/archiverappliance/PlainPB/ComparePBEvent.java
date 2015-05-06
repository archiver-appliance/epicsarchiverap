/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PartionedTime;
import edu.stanford.slac.archiverappliance.PB.search.CompareEventLine;

/**
 * A comparator for PB events that is used in searching.
 * @author mshankar
 *
 */
public class ComparePBEvent implements CompareEventLine {
	private int searchsecondsintoyear;
	private ArchDBRTypes type;
	public ComparePBEvent(ArchDBRTypes type, int secondsintoyear) {
		this.type = type;
		this.searchsecondsintoyear = secondsintoyear;
	}
	

	@Override
	public NextStep compare(byte[] line1, byte[] line2) throws IOException  {
		// The year does not matter here as we are driving solely off secondsintoyear. So we set it to 0.
		Constructor<? extends DBRTimeEvent> constructor = DBR2PBTypeMapping.getPBClassFor(type).getUnmarshallingFromByteArrayConstructor();
		short year = (short) 1970;
		int line1InputSecondsIntoYear = -1;
		int line2InputSecondsIntoYear = Integer.MAX_VALUE;
		try {
			// The raw forms for all the DBR types implement the PartionedTime interface 
			PartionedTime e = (PartionedTime) constructor.newInstance(year, new ByteArray(line1));
			line1InputSecondsIntoYear = e.getSecondsIntoYear();
			if(line2 != null) {
				PartionedTime e2 = (PartionedTime) constructor.newInstance(year, new ByteArray(line2));
				line2InputSecondsIntoYear = e2.getSecondsIntoYear();
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
		if(line1InputSecondsIntoYear < 0) {
			throw new IOException("We cannot have a negative seconds into year " + line1InputSecondsIntoYear);
		}
		if(line1InputSecondsIntoYear >= searchsecondsintoyear) {
			// System.out.println("When searching for " + searchsecondsintoyear + ", comparing with " + line1InputSecondsIntoYear + " sayz GO_LEFT");
			return NextStep.GO_LEFT;
		} else if(line2InputSecondsIntoYear < searchsecondsintoyear) {
			// System.out.println("When searching for " + searchsecondsintoyear + ", comparing with " + line2InputSecondsIntoYear + " sayz GO_RIGHT");
			return NextStep.GO_RIGHT;
		} else {
			// If we are here, line1 < SS < line2
			if(line2 != null) {
				if(line1InputSecondsIntoYear < searchsecondsintoyear && line2InputSecondsIntoYear >= searchsecondsintoyear) {
					// System.out.println("When searching for " + searchsecondsintoyear + ", comparing with " + line1InputSecondsIntoYear + " and " + line2InputSecondsIntoYear + " sayz STAY_WHERE_YOU_ARE");
					return NextStep.STAY_WHERE_YOU_ARE;
				} else {
					// System.out.println("When searching for " + searchsecondsintoyear + ", comparing with " + line1InputSecondsIntoYear + " and " + line2InputSecondsIntoYear + " sayz GO_LEFT");
					return NextStep.GO_LEFT;
				}
			} else {
				// System.out.println("When searching for " + searchsecondsintoyear + ", line 2 is null; so sayz STAY_WHERE_YOU_ARE");
				return NextStep.STAY_WHERE_YOU_ARE;
			}
		}
	}
}
