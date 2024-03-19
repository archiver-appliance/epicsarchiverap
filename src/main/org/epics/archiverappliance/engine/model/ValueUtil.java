package org.epics.archiverappliance.engine.model;

import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorValue;



public class ValueUtil {

	 public static double getDouble(final SampleValue value)
	    {
		
		  
		 if (value instanceof VectorValue )
		 {
			 return  ((VectorValue<?>)value).getValue().doubleValue();
		 }
		
		 else if (value instanceof ScalarValue )
		 {
			 return ((ScalarValue<?>)value).getValue().doubleValue();
		 }
		 else
		   return Double.NaN;
		 
	    }
	 
}
