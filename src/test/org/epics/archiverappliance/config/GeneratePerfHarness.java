package org.epics.archiverappliance.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

/**
 * Generates a standard performance SIOC database and JDBM2 persistence file in the current folder.
 * This generates
 * <ul>
 * <li>250 DBR_DOUBLE PV's changing at 10Hz</li>
 * <li>2500 DBR_DOUBLE PV's changing at 1Hz</li>
 * <li>25000 DBR_DOUBLE PV's changing at 0.1Hz</li>
 * </ul> 
 * Start the SIOC using <code>softIoc -d archapplperf.db</code>
 * Start the archiver appliance using
 * <pre>
 * <code>
 *  export ARCHAPPL_PERSISTENCE_LAYER="org.epics.archiverappliance.config.persistence.JDBM2Persistence";
 *  export ARCHAPPL_PERSISTENCE_LAYER_JDBM2FILENAME="/pathto/archapplperf.jdbm2"
 *  </code>
 *  </pre>
 * @author mshankar
 *
 */
public class GeneratePerfHarness {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String instanceName = "appliance0";
		if(args.length > 0) { 
			instanceName = args[0];
		}

		System.out.println("Generating perf harness for appliance " + instanceName);

		String siocDBFileName = "archapplperf.db";
		String jdbm2perstFileName = "archapplperf.jdbm2";
		
		File siocDB = new File(siocDBFileName);
		if(siocDB.exists()) { 
			siocDB.delete();
		}
		
		File jdbm2perst = new File(jdbm2perstFileName);
		if(jdbm2perst.exists()) { 
			jdbm2perst.delete();
		}
		

		int startingAt = 0;
		int pvcount = 250 + 2500 + 25000;
		RecordManager recMan = null;

		try(PrintWriter out = new PrintWriter(new FileOutputStream(siocDB))) { 
			recMan = RecordManagerFactory.createRecordManager(jdbm2perstFileName);
			PrimaryTreeMap<String,String> map = recMan.treeMap("TypeInfo");
			
			for(int i = startingAt; i <(startingAt+pvcount); i++) {
				String scanPeriod = "10";
				if(i < 250) { 
					scanPeriod = ".1";
				} else if(i < 2500) { 
					scanPeriod = "1";
				}
				
				
				int m=i/200;
				String pvName = "archappl" + m + ":step" + (i);
				out.println("record(calc, \"" + pvName + "\") {\n" + 
						  "field(SCAN, \"" + scanPeriod + " second\")\n" + 
						  "field(INPA, \"" + pvName + ".VAL\")\n" + 
						  "field(CALC, \"(A<1)?A+0.0001:-1\")\n" + 
						  "field(HIHI, \"0.9\")\n" + 
						  "field(HHSV, \"MAJOR\")\n" + 
						  "field(HIGH, \"0.6\")\n" + 
						  "field(HSV, \"MINOR\")\n" + 
						  "field(LOW, \"-0.3\")\n" + 
						  "field(LSV, \"MINOR\")\n" + 
						  "field(LOLO, \"-0.5\")\n" + 
						  "field(LLSV, \"MAJOR\")\n" + 
						  "field(HOPR, \"0.8\")\n" + 
						  "field(ADEL, \"0\")\n" + 
						  "field(MDEL, \"0\")\n" + 
						  "}\n\n"
						  );
				
				StringWriter jsonStr = new StringWriter();
				jsonStr.append("{\"upperDisplayLimit\":\"0.0\",\"computedBytesPerEvent\":\"19\",\"userSpecifiedEventRate\":\"0.0\",\"lowerCtrlLimit\":\"0.0\",\"lowerDisplayLimit\":\"0.0\",\"samplingPeriod\":\"0.01\",\"lowerAlarmLimit\":\"NaN\",\"computedEventRate\":\"1.0166667\",\"extraFields\":{\"NAME\":\"" + pvName + "\",\"RTYP\":\"ai\",\"ADEL\":\"0.0\",\"MDEL\":\"0.0\",\"SCAN\":\"0.0\"},\"creationTime\":\"2012-08-23T23:29:06.841Z\",\"DBRType\":\"DBR_SCALAR_DOUBLE\",\"hasReducedDataSet\":\"false\",\"upperWarningLimit\":\"NaN\",\"computedStorageRate\":\"19.383333\",\"applianceIdentity\":\"" + instanceName + "\",\"precision\":\"0.0\",\"scalar\":\"true\",\"samplingMethod\":\"MONITOR\",\"paused\":\"false\",\"elementCount\":\"1\",\"pvName\":\""+ pvName + "\",\"modificationTime\":\"2012-08-23T23:29:06.841Z\",\"upperAlarmLimit\":\"NaN\",\"upperCtrlLimit\":\"0.0\",\"units\":\"Lollipo\",\"dataStores\":[\"pb:\\/\\/localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR\",\"pb:\\/\\/localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY&hold=2&gather=1\",\"pb:\\/\\/localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR&pp=firstSample&pp=firstSample_3600\"],\"archiveFields\":[\"LOLO\",\"HIGH\",\"LOW\",\"LOPR\",\"HOPR\",\"HIHI\"],\"lowerWarningLimit\":\"NaN\"}");
				
				map.put(pvName, jsonStr.toString());
			}
		} finally { 
			if(recMan != null) { try { recMan.close(); recMan = null; } catch(Exception ex) {} } 
		}
	}	
}
