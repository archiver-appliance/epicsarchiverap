package org.epics.archiverappliance.mgmt.pva.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.mgmt.bpl.ArchivePVAction;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.pva.data.PVAStringArray;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.MustBeArrayException;
import org.epics.pva.data.nt.PVATable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Add one or more pvs to the archiver.
 * <p>
 * The requests for archiving pv's, the request consists of an NTTable with a list of pv's to be archived, optional 
 * attributes include sampling period and,or samplingmethod
 * <p>
 * example:
 * request
 * epics:nt/NTTable:1.0
 * <ul>
 *   <li>string[] labels [pv,samplingperiod,samplingmethod]</li>
 *   <li>structure value
 *   <ul>
 *       <li>string[] pv [mshankar:arch:sine,mshankar:arch:cosine]</li>
 *       <li>string[] samplingperiod [1.0,2.0]</li>
 *       <li>string[] samplingmethod [SCAN,MONITOR]</li>
 *       </ul>
 *   </li>
 *   <li>string descriptor archivePVs</li>
 * </ul>
 * <p>
 * result
 * epics:nt/NTTable:1.0
 * <ul>
 *   <li>string[] labels [pvName,status]</li>
 *   <li>structure value
 *   <ul>
 *       <li>string[] pvName [mshankar:arch:sine,mshankar:arch:cosine]</li>
 *       <li>string[] status [Archive request submitted,Archive request submitted]</li>
 *       </ul>
 *   </li>
 * </ul>
 * <p>
 * Based on {@link ArchivePVAction}
 * @author Kunal Shroff, mshankar
 *
 */
public class PvaArchivePVAction implements PvaAction {

	public static final Logger logger = LogManager.getLogger(PvaArchivePVAction.class);
	public static final String NAME = "archivePVs";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public PVAStructure request(PVAStructure args, ConfigService configService) throws PvaActionException {
		PVATable ntTable = PVATable.fromStructure(args);
		String[] pvNames = NTUtil.extractStringArray(ntTable.getColumn("pv"));
		String[] samplingperiods = NTUtil.extractStringArray(ntTable.getColumn("samplingperiod"));
		String[] samplingmethods = NTUtil.extractStringArray(ntTable.getColumn("samplingmethod"));
		String[] controllingPVs = NTUtil.extractStringArray(ntTable.getColumn("controllingPV"));
		String[] policys = NTUtil.extractStringArray(ntTable.getColumn("policy"));

		ByteArrayOutputStream result = new ByteArrayOutputStream();
		PrintWriter pw = new PrintWriter(result);
		for (int i = 0; i < pvNames.length; i++) {
			String pvName = pvNames[i];
			String samplingPeriodStr = null;
			if (samplingperiods != null && samplingperiods.length == pvNames.length && samplingperiods[i] != null) {
				samplingPeriodStr = samplingperiods[i];
			}
			boolean samplingPeriodSpecified = samplingPeriodStr != null && !samplingPeriodStr.isEmpty();
			float samplingPeriod = PolicyConfig.DEFAULT_MONITOR_SAMPLING_PERIOD;
			if(samplingPeriodSpecified) {
				samplingPeriod = Float.parseFloat(samplingPeriodStr);
			}

			SamplingMethod samplingMethod = SamplingMethod.MONITOR;
			if (samplingmethods != null && samplingmethods.length == pvNames.length && samplingmethods[i] != null) {
				samplingMethod = SamplingMethod.valueOf(samplingmethods[i]);
			}
			String controllingPV = null;
			if (controllingPVs != null && controllingPVs.length == pvNames.length) {
				controllingPV = controllingPVs[i];
				if (controllingPV != null && !controllingPV.isEmpty()) {
					logger.debug("We are conditionally archiving using controlling PV " + controllingPV);
				}
			}
			String policyName = null;
			if (policys != null && policys.length == pvNames.length) {
				policyName = policys[i];
				if (policyName != null && !policyName.equals("")) {
					logger.info("We have a user override for policy " + policyName);
				}
			}

			try {
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
			} catch (IOException e) {
				throw new PvaActionException("Archiving pv " + pvName + " failed", e);
			}
		}
		logger.info("Finished archiving pv " + Arrays.toString(pvNames));
		pw.flush();
		logger.info(result.toString());
		return parseArchivePvResult(result.toString());
	}
	
	/**
	 * example string returned
	 * { "pvName": "mshankar:arch:sine", "status": "Archive request submitted" }
	 * { "pvName": "mshankar:arch:cosine", "status": "Archive request submitted" }
	 * @param resultString input string
	 * @return Table of queries
	 */
	public static PVATable parseArchivePvResult(String resultString) throws ResponseConstructionException {
		PVATable.PVATableBuilder resultTableBuilder = PVATable.PVATableBuilder.aPVATable()
				.name(NAME)
				.descriptor(NAME + " result");
		String[] result = resultString.split("\\r?\\n");
		ArrayList<String> pvNamesResult = new ArrayList<>(result.length);
		ArrayList<String> statusResult = new ArrayList<>(result.length);

		JSONParser parser = new JSONParser();
		for (String s : Arrays.asList(result)) {
			JSONObject obj = null;
			try {
				obj = (JSONObject) parser.parse(s);
			} catch (ParseException e) {
				throw new ResponseConstructionException(e);
			}
			pvNamesResult.add(obj.get("pvName").toString());
			statusResult.add(obj.get("status").toString());
		}
		resultTableBuilder.addColumn(new PVAStringArray("pvName", pvNamesResult.toArray(new String[result.length])));
		resultTableBuilder.addColumn(new PVAStringArray("status", statusResult.toArray(new String[result.length])));
		try {
			return resultTableBuilder.build();
		} catch (MustBeArrayException e) {
			throw new ResponseConstructionException(resultString, e);
		}
	}

}
