package org.epics.archiverappliance.retrieval;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RetrievalError {

    private static final Logger logger = LogManager.getLogger(RetrievalError.class);

    public static boolean check(boolean booleanCheck, String msg, HttpServletResponse resp, int scServiceUnavailable) throws IOException {
        return check(booleanCheck, msg, resp, scServiceUnavailable, null);
    }

    public static boolean check(boolean booleanCheck, String msg, HttpServletResponse resp, int sc, Exception ex) throws IOException {
        if (booleanCheck) {
            logAndRespond(msg, ex, resp, sc);
            return true;
        }
        return false;
    }
    public static void logAndRespond(String msg, Exception ex, HttpServletResponse resp, int sc) throws IOException {
        if (ex != null) {
            logger.error(msg);
        } else {
            logger.warn(msg, ex);
        }
        resp.sendError(sc, msg);
    }
}
