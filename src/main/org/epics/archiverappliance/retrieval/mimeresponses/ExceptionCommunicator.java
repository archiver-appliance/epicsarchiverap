package org.epics.archiverappliance.retrieval.mimeresponses;

/**
 * Comminucate exceptions as part of the mime response.
 * Many exceptions during retrieval are logged at warning or below. To debug, some mime responses (like the txt response) will also send the exception to the client. 
 * @author mshankar
 *
 */
public interface ExceptionCommunicator {
	void comminucateException(Throwable t);
}
