package org.epics.archiverappliance.mgmt;

import java.util.HashMap;

import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.json.simple.JSONArray;

import com.google.common.net.UrlEscapers;

public class ArchiveWorkflowCompleted {
	@SuppressWarnings("unchecked")
	public static void isArchiveRequestComplete(String pvNameToArchive) {
		// We have to wait for a few minutes here as it does take a while for the workflow to complete.
		 for(int i = 0; i < 60; i++) { 
			try { Thread.sleep(10*1000); } catch (InterruptedException ex) {} 
			String pvsInArchiveWorkflow = "http://localhost:17665/mgmt/bpl/getNeverConnectedPVs";
			JSONArray neverConnectedPVs = GetUrlContent.getURLContentAsJSONArray(pvsInArchiveWorkflow);
			boolean workflowCompleted = true;
			for(Object neverConnectedPVObj : neverConnectedPVs) { 
				HashMap<String, String> neverConnectedPV = (HashMap<String, String>) neverConnectedPVObj;
				if(neverConnectedPV.get("pvName").equals(pvNameToArchive)) { 
					workflowCompleted = false;
				}
			}
			if(workflowCompleted) { 
				break;
			}
		 }
		 for(int i = 0; i < 60; i++) { 
			try { Thread.sleep(10*1000); } catch (InterruptedException ex) {} 
			String pvStatusURL = "http://localhost:17665/mgmt/bpl/getPVStatus?pv=" + UrlEscapers.urlFormParameterEscaper().escape(pvNameToArchive);
			HashMap<String, String> pvStatus = (HashMap<String, String>) GetUrlContent.getURLContentAsJSONArray(pvStatusURL).get(0);
			if(pvStatus.get("status").equals("Being archived")) { 
				break;
			}
		 }
	}

}
