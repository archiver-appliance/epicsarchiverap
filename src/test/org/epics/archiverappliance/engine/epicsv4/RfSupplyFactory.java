/**
 * Copyright - See the COPYRIGHT that is included with this distribution.
 * EPICS JavaIOC is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 */
package org.epics.archiverappliance.engine.epicsv4;

import org.epics.ioc.database.PVRecordField;
import org.epics.ioc.support.AbstractSupport;
import org.epics.ioc.support.Support;
import org.epics.ioc.support.SupportProcessRequester;
import org.epics.ioc.support.SupportState;
import org.epics.ioc.util.RequestResult;
import org.epics.pvData.pv.MessageType;
import org.epics.pvData.pv.PVAuxInfo;
import org.epics.pvData.pv.PVDouble;
import org.epics.pvData.pv.PVField;
import org.epics.pvData.pv.PVScalar;
import org.epics.pvData.pv.PVString;
import org.epics.pvData.pv.PVStructure;
import org.epics.pvData.pv.ScalarType;
import org.epics.pvData.pv.Type;


/**
 * Record that holds a double value, an input link, and an array of process or output links.
 * @author mrk
 *
 */
public class RfSupplyFactory {
    /**
     * Create the support for the record or structure.
     * @param pvRecordField The structure or record for which to create support.
     * @return The support instance.
     */
    public static Support create(PVRecordField pvRecordField) {
    	
    	
    	//System.out.println("-----RfSupplyFactory-----------create------");
        PVAuxInfo pvAuxInfo = pvRecordField.getPVField().getPVAuxInfo();
        PVScalar pvScalar = pvAuxInfo.getInfo("supportFactory");
        if(pvScalar==null) {
            pvRecordField.message("no pvAuxInfo with name support. Why??", MessageType.error);
            return null;
        }
        if(pvScalar.getScalar().getScalarType()!=ScalarType.pvString) {
            pvRecordField.message("pvAuxInfo for support is not a string. Why??", MessageType.error);
            return null;
        }
        String supportName = ((PVString)pvScalar).get();
       
        if(!supportName.equals(powerSupplyFactory)) {
            pvRecordField.message("no support for " + supportName, MessageType.fatalError);
          //  System.out.println("--supportName---TEST-----"+supportName);
            return null;
        }
        // we want the parent of the parent
        PVStructure pvParent = pvRecordField.getPVField().getParent();
      //  System.out.println(pvParent);
        if(pvParent==null) {
            pvRecordField.message("no parent", MessageType.fatalError);
            return null;
        }
        pvParent = pvParent.getParent();
        if(pvParent==null) {
            pvRecordField.message("no parent of the parent", MessageType.fatalError);
            return null;
        }
        PVDouble phase = getPVDouble(pvParent,"value.phase");
        if(phase==null) return null;
        PVDouble amplitude = getPVDouble(pvParent,"value.amplitude");
        if(amplitude==null) return null;
       // PVDouble pvPower = getPVDouble(pvParent,"power.value");
       // if(pvPower==null) return null;
       
        return new PowerSupplyCurrentImpl(pvRecordField,phase,amplitude);
    }
    
    private static PVDouble getPVDouble(PVStructure pvParent,String fieldName) {
        PVField pvField = pvParent.getSubField(fieldName);
        if(pvField==null) {
            pvParent.message(fieldName + " does not exist", MessageType.fatalError);
            return null;
        }
        if(pvField.getField().getType()!=Type.scalar) {
            pvParent.message(fieldName + " is not a double", MessageType.fatalError);
            return null;
        }
        PVScalar pvScalar = (PVScalar)pvField;
        if(pvScalar.getScalar().getScalarType()!=ScalarType.pvDouble) {
            pvParent.message(fieldName + " is not a double", MessageType.fatalError);
            return null;
        }
        return (PVDouble)pvField;
    }
    
    private static final String powerSupplyFactory = "org.epics.ioc.rfSupplyFactory";
    
    
    static private class PowerSupplyCurrentImpl extends AbstractSupport
    {
        private PVDouble phasePVField = null;
        private PVDouble amplitudePVField = null;
        
        private double phaseDouble;
        private double amplitudeDouble;
        
        private PowerSupplyCurrentImpl(PVRecordField pvRecordField,PVDouble phasePVField, PVDouble amplitudePVField) {
            super(powerSupplyFactory,pvRecordField);
            this.phasePVField = phasePVField;
            this.amplitudePVField = amplitudePVField;
        }
        /* (non-Javadoc)
         * @see org.epics.ioc.process.Support#process(org.epics.ioc.process.RecordProcessRequester)
         */
        @Override
        public void process(SupportProcessRequester supportProcessRequester) {
        	if(!super.checkSupportState(SupportState.ready,"process")) {
        		supportProcessRequester.supportProcessDone(RequestResult.failure);
        		return;
        	}
        	phaseDouble = phasePVField.get();
        	amplitudeDouble = amplitudePVField.get();
        	if(phaseDouble>100)phaseDouble=0;
        	if(amplitudeDouble>100)amplitudeDouble=0;

        	amplitudePVField.put(amplitudeDouble+1);
        	phasePVField.put(amplitudeDouble+5.0);
        	supportProcessRequester.supportProcessDone(RequestResult.success);
        }
    }
}
