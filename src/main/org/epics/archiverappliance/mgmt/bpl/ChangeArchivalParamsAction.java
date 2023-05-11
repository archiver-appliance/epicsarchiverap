package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Change the archival parameters for a PV. 
 * For now, we support changing the sampling period.
 * 
 * @epics.BPLAction - Change the archival parameters for a PV. 
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionParam samplingperiod - The new sampling period in seconds. 
 * @epics.BPLActionParam samplingmethod - The new sampling method - For now, this is one of SCAN or MONITOR.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class ChangeArchivalParamsAction implements BPLAction {
	private static Logger logger = LogManager.getLogger(ChangeArchivalParamsAction.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String samplingperiodStr = req.getParameter("samplingperiod");
		if(samplingperiodStr == null || samplingperiodStr.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String samplingMethodStr = req.getParameter("samplingmethod");

		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();

		// String pvNameFromRequest = pvName;
		String realName = configService.getRealNameForAlias(pvName);
		if(realName != null) pvName = realName;

		ApplianceInfo info = configService.getApplianceForPV(pvName);
		if(info == null) {
			infoValues.put("validation", "Trying to change the parameters for PV " + pvName + " that is not currently being archived.");
			logger.error(infoValues.get("validation"));
			try(PrintWriter out = resp.getWriter()) {
				infoValues.put("validation", "Unable to change the archival parameters for " + pvName);
				out.println(JSONValue.toJSONString(infoValues));
			}
		} else {
			PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
			assert(typeInfo != null);

			SamplingMethod samplingMethod = typeInfo.getSamplingMethod();
			if(samplingMethodStr != null) {
				samplingMethod = SamplingMethod.valueOf(samplingMethodStr);
			}
			
			float samplingPeriod = Float.parseFloat(samplingperiodStr);
			typeInfo.setSamplingPeriod(samplingPeriod);
			typeInfo.setSamplingMethod(samplingMethod);
			typeInfo.setModificationTime(TimeUtils.now());
			configService.updateTypeInfoForPV(pvName, typeInfo);

			logger.info("Changing the archival parameters for PV " + pvName + ". Changing sampling period to " + samplingperiodStr + " and sampling method " + samplingMethod.toString());
			
			if(!typeInfo.isPaused()) { 
				String pvStatusURLStr = info.getEngineURL() + "/changeArchivalParameters" 
						+ "?pv=" + URLEncoder.encode(pvName, "UTF-8") 
						+ "&samplingperiod=" + URLEncoder.encode(samplingperiodStr, "UTF-8")
						+ "&samplingmethod=" + URLEncoder.encode(typeInfo.getSamplingMethod().toString(), "UTF-8")
						+ "&dest=" + URLEncoder.encode(typeInfo.getDataStores()[0], "UTF-8")
						+ "&usePVAccess=" + Boolean.toString(typeInfo.isUsePVAccess())
						+ "&useDBEProperties=" + Boolean.toString(typeInfo.isUseDBEProperties());
				JSONObject pvStatus = GetUrlContent.getURLContentAsJSONObject(pvStatusURLStr);
				if(pvStatus != null && !pvStatus.equals("")) {
					try(PrintWriter out = resp.getWriter()) {
						out.println(JSONValue.toJSONString(pvStatus));
					}
				} else {
					try(PrintWriter out = resp.getWriter()) {
						infoValues.put("validation", "Unable to change the archival parameters for " + pvName);
						out.println(JSONValue.toJSONString(infoValues));
					}
				}
			}
		}		
	}

}
