package org.epics.archiverappliance.retrieval.bpl;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

/**
 * 
 * Given a list of PV's in the POST, filter these to those that are being archived in this cluster.
 * @author mshankar
 *
 */
public class FilterArchivedPVs implements BPLAction {
	private static Logger logger = LogManager.getLogger(FilterArchivedPVs.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {

		resp.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		
		String contentType = req.getContentType();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (InputStream is = new BufferedInputStream(req.getInputStream())) {
			IOUtils.copy(is, bos);
		}
		bos.flush();
		bos.close();
		byte[] pvNamesBytes = bos.toByteArray();
		
		if(pvNamesBytes.length <= 0) {
			logger.error("PV list is empty when trying to filter archived PVs");
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		// We pass on the request to the mgmt webapps in the cluster.
		List<String> filteredPVs = new LinkedList<String>(); 
		LinkedList<String> mgmtURLs = getMgmtURLsInCluster(configService);
		for(String mgmtUrl : mgmtURLs) { 
			URL retrievalMatchNamesURL = new URL(mgmtUrl + "/archivedPVsForThisAppliance");
			HttpURLConnection conn = (HttpURLConnection) retrievalMatchNamesURL.openConnection();
			conn.setRequestMethod("POST");
	        conn.setRequestProperty("Content-Type", contentType);
	        conn.setRequestProperty("Content-Length", String.valueOf(pvNamesBytes.length));
			conn.setDoOutput(true);
			try {
				OutputStream os = conn.getOutputStream();
				os.write(pvNamesBytes);
				os.flush();
				logger.debug("Made the call to the appliance " + retrievalMatchNamesURL);
				
				
				BufferedReader reader = new BufferedReader(new InputStreamReader((conn.getInputStream()), StandardCharsets.UTF_8));
				JSONParser parser=new JSONParser();
				@SuppressWarnings("unchecked")
				List<String> respJSONObj = (List<String>) parser.parse(reader);
				filteredPVs.addAll(respJSONObj);
				logger.debug("Done parsing response from  " + retrievalMatchNamesURL);
		        
		        os.close();
		        reader.close();
			} catch(Exception ex) { 
				logger.error("Exception filtering names from " + retrievalMatchNamesURL, ex);
			} 
			finally { 
		        conn.disconnect();				
			}
		}

		
		try (PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(filteredPVs));
		} catch(Exception ex) {
			logger.error("Exception sending filtered output " + configService.getMyApplianceInfo().getIdentity(), ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
	
	
	private LinkedList<String> getMgmtURLsInCluster(ConfigService configService) {
		LinkedList<String> mgmtURLs = new LinkedList<String>();
		try { 
			JSONArray appliancesInCluster = GetUrlContent.getURLContentAsJSONArray(configService.getMyApplianceInfo().getMgmtURL() + "/getAppliancesInCluster");
			for(Object object : appliancesInCluster) { 
				mgmtURLs.add((String)((JSONObject)object).get("mgmtURL"));
			}
			logger.debug("Returning " + mgmtURLs.size() + " mgmt URLs");
			return mgmtURLs;
		} catch(Throwable t) { 
			logger.error("Exception determining the appliances in the cluster", t);
			return null;
		}
	}
}
