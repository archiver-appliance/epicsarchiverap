/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv;



import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;
import gov.aps.jca.dbr.DBR_Byte;
import gov.aps.jca.dbr.DBR_Double;
import gov.aps.jca.dbr.DBR_Float;
import gov.aps.jca.dbr.DBR_Int;
import gov.aps.jca.dbr.DBR_Short;
import gov.aps.jca.dbr.DBR_TIME_Enum;



;


/** Helper for dealing with DBR.. types.
 *  <p>
 *  JCA provides up to "...Int", returning an int/Integer.
 *  IValue uses long for future protocol support.
 *
 *  @author Kay Kasemir
 */
public class DBR_Helper
{


    /** @return TIME_... type for this channel. */
    public static DBRType getTimeType(final boolean plain, final DBRType type)
    {
        if (type.isDOUBLE())
            return plain ? DBRType.DOUBLE : DBRType.TIME_DOUBLE;
        else if (type.isFLOAT())
            return plain ? DBRType.FLOAT : DBRType.TIME_FLOAT;
        else if (type.isINT())
            return plain ? DBRType.INT : DBRType.TIME_INT;
        else if (type.isSHORT())
            return plain ? DBRType.SHORT : DBRType.TIME_SHORT;
        else if (type.isENUM())
            return plain ? DBRType.SHORT : DBRType.TIME_ENUM;
        else if (type.isBYTE())
            return plain ? DBRType.BYTE: DBRType.TIME_BYTE;
        // default: get as string
        return plain ? DBRType.STRING : DBRType.TIME_STRING;
    }
    
    
    
    public static boolean decodeBooleanValue(final DBR dbr) throws Exception
		{
    	boolean notEqual0=true;
		if (dbr.isDOUBLE())
		{
		 double v[];
		 v = ((DBR_Double)dbr).getDoubleValue();
		 if(v[0]==0)
		 {
			 notEqual0=false;
		 }
		}
		
		else if (dbr.isFLOAT())
		{
		float v[];
	
		 v = ((DBR_Float)dbr).getFloatValue();
		 if(v[0]==0)
		 {
			 notEqual0=false;
		 }
		}
		else if (dbr.isINT())
		{
		int v[];
		
		 v = ((DBR_Int)dbr).getIntValue();
		 if(v[0]==0)
		 {
			 notEqual0=false;
		 }
		
		}
		else if (dbr.isSHORT())
		{
		short v[];
		
		 v = ((DBR_Short)dbr).getShortValue();
		 if(v[0]==0)
		 {
			 notEqual0=false;
		 }
		}
		else if (dbr.isENUM())
		{
		short v[];
		// 'plain' mode would subscribe to SHORT,
		// so this must be a TIME_Enum:
		final DBR_TIME_Enum dt = (DBR_TIME_Enum) dbr;
		v = dt.getEnumValue();
		if(v[0]==0)
		 {
			 notEqual0=false;
		 }
		}
		else if (dbr.isBYTE())
		{
		byte[] v;
		
		 v = ((DBR_Byte)dbr).getByteValue();
		 if(v[0]==0)
		 {
			 notEqual0=false;
		 }
		}
		else
		// handle many more types!!
		throw new Exception("Cannot decode " + dbr);
		
		 return notEqual0;
		}
   
    public static DBRType getControlType(final DBRType type)
    {
		if (type.isDOUBLE() || type.isFLOAT())
			return DBRType.CTRL_DOUBLE;
		else if (type.isENUM())
			return DBRType.LABELS_ENUM;
		else if (type.isINT())
			return DBRType.CTRL_INT;
		else
			return DBRType.CTRL_SHORT;
    }
    
}
