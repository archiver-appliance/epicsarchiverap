package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EPICS_V3_PV;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Use this to debug any connectivity issues in the engine. 
 * This print outs the details of the ArchiveChannel/EPICS_PV for a given PV as JSON
 * @author mshankar
 *
 */
public class ArchiveChannelObjectDetailsAction implements BPLAction {
	private static Logger logger = Logger.getLogger(ArchiveChannelObjectDetailsAction.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		ArchiveChannel channel = configService.getEngineContext().getChannelList().get(pvName);
		if(channel == null) { 
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		try {
			infoValues.put("channel", objectToJSON(pvName, channel, true));
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(infoValues));
			}
		} catch(Exception ex) { 
			logger.error("Exception reflecting field for pv " + pvName, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
	}
	public HashMap<String, Object> objectToJSON(String pvName, Object obj, boolean useSuperClassFields) throws IllegalAccessException {
		HashMap<String, Object> objValues = new HashMap<String, Object>();
		Class<?> theClass = null;
		Field[] objFields = null;
		if(useSuperClassFields) {
			theClass = obj.getClass().getSuperclass();
			objFields = obj.getClass().getSuperclass().getDeclaredFields();
		} else { 
			theClass = obj.getClass();
			objFields = obj.getClass().getDeclaredFields();
		}
		logger.debug("Channel for " + pvName + " of class " + theClass.getCanonicalName() + " has " + objFields.length + " declared fields");
		for(Field objField : objFields) {
			objField.setAccessible(true);
			if(objField.getType().isPrimitive() || objField.getType().isEnum()) {
				objValues.put(objField.getName(), objField.get(obj).toString());
			} else if (String.class.isInstance(objField.get(obj))) {
				String stringVal = (String) objField.get(obj);
				objValues.put(objField.getName(), stringVal);
			} else if (DBRTimeEvent.class.isInstance(objField.get(obj))) {
				DBRTimeEvent timeEvent = (DBRTimeEvent) objField.get(obj);
				if(timeEvent != null) { 
					objValues.put(objField.getName(), TimeUtils.convertToHumanReadableString(timeEvent.getEventTimeStamp()));
				}
			} else if (EPICS_V3_PV.class.isInstance(objField.get(obj))) { 
				EPICS_V3_PV pv = (EPICS_V3_PV) objField.get(obj);
				if(pv != null) { 
					objValues.put("epics_v3_pv", objectToJSON(pvName, pv, false));
				}
			} else { 
				objValues.put(objField.getName(), (objField.get(obj) == null ? "null" : "non-null"));
			}
		}
		return objValues;
	}
}
