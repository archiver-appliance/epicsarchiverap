package org.epics.archiverappliance.mgmt.policy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;

/**
 * Returns the text of the policies for this installation as a text file...
 * @author mshankar
 *
 */
public class GetPolicyText implements BPLAction {

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,ConfigService configService) throws IOException {
		try(InputStream is = configService.getPolicyText()) {
			resp.setContentType("text/plain");
			try(OutputStream os = resp.getOutputStream()) {
				byte[] buf = new byte[10*1024];
				int bytesRead = is.read(buf);
				while(bytesRead > 0) {
					os.write(buf, 0, bytesRead);
					bytesRead = is.read(buf);
				}
			}
		}
	}
}
