package org.epics.archiverappliance.mgmt.pva.actions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.ArchivePVAction;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.nt.NTTable;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Add one or more pvs to the archiver.
 * 
 * The requests for archiving pv's, the request consists of an NTTable with a list of pv's to be archived, optional 
 * attributes include sampling period and,or samplingmethod
 * 
 * example:
 * request
 * epics:nt/NTTable:1.0 
 *   string[] labels [pv,samplingperiod,samplingmethod]
 *   structure value
 *       string[] pv [mshankar:arch:sine,mshankar:arch:cosine]
 *       string[] samplingperiod [1.0,2.0]
 *       string[] samplingmethod [SCAN,MONITOR]
 *   string descriptor archivePVs
 *
 * result
 * epics:nt/NTTable:1.0 
 *      string[] labels [pvName,status]
 *           structure value
 *                  string[] pvName [mshankar:arch:sine,mshankar:arch:cosine]
 *                  string[] status [Archive request submitted,Archive request submitted]
 *
 * Based on {@link ArchivePVAction}
 * @author Kunal Shroff, mshankar
 *
 */
public class PvaArchivePVAction implements PvaAction {

	public static final Logger logger = Logger.getLogger(PvaArchivePVAction.class);
	public static final String NAME = "archivePVs";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public void request(PVStructure args, RPCResponseCallback callback, ConfigService configService) {
		NTTable ntTable = NTTable.wrap(args);
		String[] pvNames = NTUtil.extractStringArray(ntTable.getColumn(PVStringArray.class, "pv"));
		String[] samplingperiods = NTUtil.extractStringArray(ntTable.getColumn(PVStringArray.class, "samplingperiod"));
		String[] samplingmethods = NTUtil.extractStringArray(ntTable.getColumn(PVStringArray.class, "samplingmethod"));
		String[] controllingPVs = NTUtil.extractStringArray(ntTable.getColumn(PVStringArray.class, "controllingPV"));
		String[] policys = NTUtil.extractStringArray(ntTable.getColumn(PVStringArray.class, "policy"));

		try (	ByteArrayOutputStream result = new ByteArrayOutputStream();
				PrintWriter pw = new PrintWriter(result);) {
			for (int i = 0; i < pvNames.length; i++) {
				String pvName = pvNames[i];
				String samplingPeriodStr = null;
				if (samplingperiods != null && samplingperiods[i] != null) {
					samplingPeriodStr = samplingperiods[i];
				}
				boolean samplingPeriodSpecified = samplingPeriodStr != null && !samplingPeriodStr.equals("");
				float samplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
				if(samplingPeriodSpecified) {
					samplingPeriod = Float.parseFloat(samplingPeriodStr);
				}

				SamplingMethod samplingMethod = SamplingMethod.MONITOR;
				if(samplingmethods != null && samplingmethods[i] != null) {
					samplingMethod = SamplingMethod.valueOf(samplingmethods[i]);
				}
				String controllingPV = null;
				if (controllingPVs != null) {
					controllingPV = controllingPVs[i];
					if (controllingPV != null && !controllingPV.equals("")) {
						logger.debug("We are conditionally archiving using controlling PV " + controllingPV);
					}
				}
				String policyName = null;
				if (policys != null) {
					policyName = policys[i];
					if (policyName != null && !policyName.equals("")) {
						logger.info("We have a user override for policy " + policyName);
					}
				}

				System.out.println("AAA: ");
				ArchivePVAction.archivePV(pw,
						pvName,
						samplingPeriodSpecified,
						samplingMethod,
						samplingPeriod,
						controllingPV,
						policyName,
						null,
						false,
						configService,
						ArchivePVAction.getFieldsAsPartOfStream(configService));
			}
			System.out.println("Finished archive pv");
			pw.flush();
			System.out.println(result.toString());
			callback.requestDone(StatusFactory.getStatusCreate().getStatusOK(),
					parseArchivePvResult(result.toString()).getPVStructure());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			
		}
	}
	
	/**
	 * example string returned
	 * { "pvName": "mshankar:arch:sine", "status": "Archive request submitted" }
	 * { "pvName": "mshankar:arch:cosine", "status": "Archive request submitted" }
	 * @param resultString
	 * @return
	 */
	public static NTTable parseArchivePvResult(String resultString) {
		NTTable resultTable = NTTable.createBuilder()
				.addDescriptor()
				.addColumn("pvName", ScalarType.pvString)
				.addColumn("status", ScalarType.pvString)
				.create();
		resultTable.getDescriptor().put(NAME+" result");
		String[] result = resultString.split("\\r?\\n");
		ArrayList<String> pvNamesResult = new ArrayList<>(result.length);
		ArrayList<String> statusResult = new ArrayList<>(result.length);

		JSONParser parser = new JSONParser();
		Arrays.asList(result).forEach((s) -> {
			try {
				// TODO exceptions should be better handled in order to ensure the 
				// result table is valid
				JSONObject obj = (JSONObject) parser.parse(s);
				pvNamesResult.add(obj.get("pvName").toString());
				statusResult.add(obj.get("status").toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
		});
		resultTable.getColumn(PVStringArray.class, "pvName").put(0, result.length,
				pvNamesResult.toArray(new String[result.length]), 0);
		resultTable.getColumn(PVStringArray.class, "status").put(0, result.length,
				statusResult.toArray(new String[result.length]), 0);
		return resultTable;
	}

}
