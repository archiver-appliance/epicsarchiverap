package org.epics.archiverappliance.engine.LargeSIOC;

public class GenerateIOCFile4EPICSV4 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
		if(args.length < 2) {
			System.err.println("Usage: java org.epics.archiverappliance.engine.LargeSIOC.GenerateIOCFile4EPICSV4 <StartingAt> <PVCount>");
			return;
		}
		
		int startingAt = Integer.parseInt(args[0]);
		int pvcount = Integer.parseInt(args[1]);
		System.out.println("<?xml version=\"1.0\" ?>\n<database>\n");
		for(int i = startingAt; i < (startingAt+pvcount); i++) {
			 System.out.println(
					"<record recordName = \"rf_"+i+"\" >\n"+
			  "<structure name = \"scan\" extends = \"scan\">\n"+
			        "<structure name = \"type\"><scalar name = \"choice\">periodic</scalar></structure>\n"+
			        "<scalar name =\"rate\">1.0</scalar>\n"+
			    "</structure>\n"+
			    "<structure name = \"value\">\n"+
			        "<auxInfo name = \"supportFactory\" scalarType =\"string\">org.epics.ioc.genericFactory</auxInfo>\n"+
			        "<scalar name = \"phase\" scalarType = \"double\">\n"+
			         "<auxInfo name = \"supportFactory\" scalarType = \"string\">org.epics.ioc.rfSupplyFactory</auxInfo>\n"+
			        " </scalar>\n"+
			        "<scalar name = \"amplitude\" scalarType = \"double\"/>\n"+
			       " </structure>\n"+
			          "<structure name = \"alarm\" extends = \"alarm\" />\n"+
			            "<structure name = \"timeStamp\" extends = \"timeStamp\" />\n"+
			"</record>\n"        
					
					  );
			
		}//end for
		
		System.out.println("</database>\n");

	}

}
