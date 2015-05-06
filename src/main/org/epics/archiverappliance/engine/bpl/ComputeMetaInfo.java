package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.metadata.MetaCompletedListener;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Computes some information about the pv to be archived like event rate/storage rate.
 * @author mshankar
 *
 */
public class ComputeMetaInfo implements BPLAction {
	private static Logger logger = Logger.getLogger(ComputeMetaInfo.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			logger.error("Unable to determine pv name when computing pv archive information");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		if(completedPVs.containsKey(pvName)) {
			try {
				MetaInfo metaInfo = completedPVs.get(pvName);
				if(logger.isDebugEnabled()) {
					logger.debug("Found archive info for pv " + pvName + " = " + metaInfo.toString());
				}
				JSONEncoder<MetaInfo> encoder = JSONEncoder.getEncoder(MetaInfo.class);
				JSONObject metaInfoObj = encoder.encode(metaInfo);
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				try (PrintWriter out = resp.getWriter()) { 
					out.print(JSONValue.toJSONString(metaInfoObj));
				}
				completedPVs.remove(pvName);
				return;
			} catch(Exception ex) {
				logger.error("Exception transmitting completed archive info for pv" + pvName, ex);
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;
			}
		}
		
		if(!pendingPVs.contains(pvName)) {
			try {
				String[] extraFields = configService.getExtraFields();

				logger.debug("Getting archive info for pv " + pvName);
				ArchiveEngine.getArchiveInfo(pvName, configService, extraFields, new ArchivePVMetaCompletedListener(pvName));
				pendingPVs.add(pvName);
				HashMap<String, String> emptyRet = new HashMap<String, String>();
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				try (PrintWriter out = resp.getWriter()) { 
					out.print(JSONValue.toJSONString(emptyRet));
				}
				return;
			} catch(Exception ex) {
				logger.error("Exception when computing archival info for pv " + pvName, ex);
				resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return;
			}
		} else {
			logger.debug("Still pending getting archive info for " + pvName);
			HashMap<String, String> emptyRet = new HashMap<String, String>();
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try (PrintWriter out = resp.getWriter()) { 
				out.print(JSONValue.toJSONString(emptyRet));
			}
			return;
		}
	}
	
	static private ConcurrentSkipListSet<String> pendingPVs = new ConcurrentSkipListSet<String>();
	static private ConcurrentHashMap<String, MetaInfo> completedPVs = new ConcurrentHashMap<String, MetaInfo>();
	
	static class ArchivePVMetaCompletedListener implements MetaCompletedListener {
		String pvName;
		ArchivePVMetaCompletedListener(String pvName) {
			this.pvName = pvName;
		}
		
		
		@Override
		public void completed(MetaInfo metaInfo) {
			logger.debug("Completed computing archive info for pv " + pvName);
			completedPVs.put(pvName, metaInfo);
			pendingPVs.remove(pvName);
		}
	}
}
