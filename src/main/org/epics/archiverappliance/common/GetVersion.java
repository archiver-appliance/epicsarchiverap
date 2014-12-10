package org.epics.archiverappliance.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;

/**
 * Get the version from the version.txt file.
 * @author mshankar
 *
 */
public class GetVersion implements BPLAction {
	private static Logger logger = Logger.getLogger(GetVersion.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			HashMap<String, String> output = new HashMap<String, String>();
			Path versionPath = Paths.get(req.getServletContext().getRealPath("ui/comm/version.txt"));
			String versionString = Files.readAllLines(versionPath, Charset.defaultCharset()).get(0);
			output.put("version", versionString);
			out.println(JSONObject.toJSONString(output));
		} catch(Exception ex) {
			logger.error("Exception getting version information", ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}
