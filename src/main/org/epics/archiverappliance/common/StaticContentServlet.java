package org.epics.archiverappliance.common;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.STARTUP_SEQUENCE;
import org.epics.archiverappliance.mgmt.bpl.SyncStaticContentHeadersFooters;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;

/**
 * Serves static content in the web app...
 * Previously, org.apache.catalina.servlets.DefaultServlet was used for this purpose.
 * But this ties us to Tomcat and some expressed the desire to run this in other containers.
 * In addition, we needed the ability to serve content from within zip files.
 * This lets us upgrade JavaScript libraries easily; many of which are delivered a multiple files in a versioned zip.
 * 
 * This is code from http://balusc.blogspot.com/2009/02/fileservlet-supporting-resume-and.html substantially modified.
 * @author mshankar
 * 
 *
 */
public class StaticContentServlet extends HttpServlet {
	private static final long serialVersionUID = 0L;
	private static Logger logger = LogManager.getLogger(StaticContentServlet.class.getName());
	private static final int DEFAULT_BUFFER_SIZE = 10240;
	// We expire content in this many minutes
	private static final long DEFAULT_EXPIRE_TIME = 10*60*1000L;
	
	private ConfigService configService = null;
	private String staticContentBasePath = "ui";
	/**
	 * List of paths for which we have to do template replacement
	 */
	private Set<String> templateReplacementPaths = new HashSet<String>();
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		this.configService = (ConfigService) config.getServletContext().getAttribute(ConfigService.CONFIG_SERVICE_NAME);
		templateReplacementPaths.add("viewer/index.html");
		templateReplacementPaths.add("js/mgmt.js");
	}

	@Override
	protected void doHead(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response, false);
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response, true);
	}

	/**
	 * Process the actual request.
	 * @param request The request to be processed.
	 * @param response The response to be created.
	 * @param content Whether the request body should be written (GET) or not (HEAD).
	 * @throws IOException If something fails at I/O level.
	 */
	private void processRequest (HttpServletRequest request, HttpServletResponse response, boolean content) throws IOException {
		// Validate the requested file ------------------------------------------------------------
		
		// Get requested file by path info - remove the leading '/'
		String requestedFile = request.getPathInfo();
		if(requestedFile == null || requestedFile.equals("")) { 
			logger.debug("Default request - send to index.html");
			response.setStatus(HttpServletResponse.SC_MOVED_PERMANENTLY);
			response.setHeader("Location", "index.html");
			return;
		}
		
		if(requestedFile.startsWith("/")) { 
			requestedFile = requestedFile.substring(1, requestedFile.length());
		}
		logger.debug("Procesing static content request for " + requestedFile);
		

		// Check if file is actually supplied to the request URL.
		if (requestedFile == null) {
			logger.error("Static content request for a null file?");
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		if(configService.getStartupState() != STARTUP_SEQUENCE.STARTUP_COMPLETE) { 
			String msg = "Cannot process static content request for " + requestedFile + " until the appliance has completely started up.";
			logger.error(msg);
			response.addHeader(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, msg);
			return;
		}

		
		// URL-decode the file name (might contain spaces and on) and prepare file object.
		String decodedFilePath = URLDecoder.decode(requestedFile, "UTF-8");
		
		try(PathSequence pathSeq = new PathSequence(request.getServletContext(), staticContentBasePath, decodedFilePath)) { 


			// Check if file actually exists in filesystem.
			if (!pathSeq.exists()) {
				logger.warn("Static content request for a non existent file " + decodedFilePath);
				response.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			
			logger.debug("Serving static content: " + pathSeq.toString());
			
			// Prepare some variables. The ETag is an unique identifier of the file.
			String fileName = pathSeq.getContentDispositionFileName();
			long length = pathSeq.length();
			long lastModified = pathSeq.lastModified();
			String eTag = fileName + "_" + length + "_" + lastModified;
			long expires = System.currentTimeMillis() + DEFAULT_EXPIRE_TIME;
			
//			if(logger.isDebugEnabled()) { 
//				for(String headerName : Collections.list(request.getHeaderNames())) { 
//					logger.debug(headerName + " : " + request.getHeaders(headerName).nextElement());
//				}
//			}

			// Validate request headers for caching ---------------------------------------------------
			

			// If-None-Match header should contain "*" or ETag. If so, then return 304.
			String ifNoneMatch = request.getHeader("If-None-Match");
			if (ifNoneMatch != null && matches(ifNoneMatch, eTag)) {
				logger.debug("Matched If-None-Match " + ifNoneMatch + " eTag " + eTag);
				response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
				response.setHeader("ETag", eTag); // Required in 304.
				response.setDateHeader("Expires", expires); // Postpone cache with 1 week.
				return;
			}

			// If-Modified-Since header should be greater than LastModified. If so, then return 304.
			// This header is ignored if any If-None-Match header is specified.
			long ifModifiedSince = request.getDateHeader("If-Modified-Since");
			if (ifNoneMatch == null && ifModifiedSince != -1 && ifModifiedSince + 1000 > lastModified) {
				logger.debug("Matched If-Modified-Since");
				response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
				response.setHeader("ETag", eTag); // Required in 304.
				response.setDateHeader("Expires", expires); // Postpone cache with 1 week.
				return;
			}


			// Validate request headers for resume ----------------------------------------------------

			// If-Match header should contain "*" or ETag. If not, then return 412.
			String ifMatch = request.getHeader("If-Match");
			if (ifMatch != null && !matches(ifMatch, eTag)) {
				logger.debug("If-Match did not match");
				response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED);
				return;
			}

			// If-Unmodified-Since header should be greater than LastModified. If not, then return 412.
			long ifUnmodifiedSince = request.getDateHeader("If-Unmodified-Since");
			if (ifUnmodifiedSince != -1 && ifUnmodifiedSince + 1000 <= lastModified) {
				logger.debug("If-Unmodified-Since did not match");
				response.sendError(HttpServletResponse.SC_PRECONDITION_FAILED);
				return;
			}


			// Prepare and initialize response --------------------------------------------------------
			// Get content type by file name and set default GZIP support and content disposition.
			String contentType = request.getServletContext().getMimeType(fileName);
			boolean acceptsGzip = false;
			String disposition = "inline";

			// If content type is unknown, then set the default value.
			// For all content types, see: http://www.w3schools.com/media/media_mimeref.asp
			// To add new content types, add new mime-mapping entry in web.xml.
			if (contentType == null) {
				contentType = "application/octet-stream";
			}

			// If content type is text, then determine whether GZIP content encoding is supported by
			// the browser and expand content type with the one and right character encoding.
			if (contentType.startsWith("text")) {
				String acceptEncoding = request.getHeader("Accept-Encoding");
				acceptsGzip = acceptEncoding != null && accepts(acceptEncoding, "gzip");
				contentType += ";charset=UTF-8";
			} else if (!contentType.startsWith("image")) {
				// Else, expect for images, determine content disposition. If content type is supported by
				// the browser, then set to inline, else attachment which will pop a 'save as' dialogue.
				String accept = request.getHeader("Accept");
				disposition = accept != null && accepts(accept, contentType) ? "inline" : "attachment";
			}

			// Initialize response.
			response.reset();
			response.setBufferSize(DEFAULT_BUFFER_SIZE);
			response.setHeader("Content-Disposition", disposition + ";filename=\"" + fileName + "\"");
			response.setHeader("ETag", eTag);
			response.setDateHeader("Last-Modified", lastModified);
			response.setDateHeader("Expires", expires);
			response.addHeader("ARCHAPPL_SRC", pathSeq.toString());


			// Prepare streams.
			InputStream input = null;
			OutputStream output = null;
			try {
				// Open streams.
				input = pathSeq.getInputStream();
				output = response.getOutputStream();
				response.setContentType(contentType);

				if (content) {
					if (acceptsGzip) {
						// The browser accepts GZIP, so GZIP the content.
						response.setHeader("Content-Encoding", "gzip");
						output = new GZIPOutputStream(output, DEFAULT_BUFFER_SIZE);
					} else {
						// Content length is not directly predictable in case of GZIP.
						// So only add it if there is no means of GZIP, else browser will hang.
						response.setHeader("Content-Length", String.valueOf(length));
					}

					copy(pathSeq, input, output, length);
				}
			} finally {
				// Gently close streams.
				close(output);
				close(input);
			}
		}
	}

	// Helpers (can be refactored to public utility class) ----------------------------------------

	/**
	 * Returns true if the given accept header accepts the given value.
	 * @param acceptHeader The accept header.
	 * @param toAccept The value to be accepted.
	 * @return True if the given accept header accepts the given value.
	 */
	private static boolean accepts(String acceptHeader, String toAccept) {
		String[] acceptValues = acceptHeader.split("\\s*(,|;)\\s*");
		Arrays.sort(acceptValues);
		return Arrays.binarySearch(acceptValues, toAccept) > -1
				|| Arrays.binarySearch(acceptValues, toAccept.replaceAll("/.*$", "/*")) > -1
				|| Arrays.binarySearch(acceptValues, "*/*") > -1;
	}

	/**
	 * Returns true if the given match header matches the given value.
	 * @param matchHeader The match header.
	 * @param toMatch The value to be matched.
	 * @return True if the given match header matches the given value.
	 */
	private static boolean matches(String matchHeader, String toMatch) {
		String[] matchValues = matchHeader.split("\\s*,\\s*");
		Arrays.sort(matchValues);
		return Arrays.binarySearch(matchValues, toMatch) > -1
				|| Arrays.binarySearch(matchValues, "*") > -1;
	}

	/**
	 * Copy the given input to the given output.
	 * @param input The input to copy from
	 * @param output The output to copy to.
	 * @param length Number of bytes to copy.
	 * @throws IOException If something fails at I/O level.
	 */
	private static void copy(PathSequence pathSeq, InputStream input, OutputStream output, long length) throws IOException {
		byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
		int read;

		if (pathSeq.length() == length) {
			while ((read = input.read(buffer)) > 0) {
				output.write(buffer, 0, read);
			}
		} else {
			long toRead = length;

			while ((read = input.read(buffer)) > 0) {
				if ((toRead -= read) > 0) {
					output.write(buffer, 0, read);
				} else {
					output.write(buffer, 0, (int) toRead + read);
					break;
				}
			}
		}
	}

	/**
	 * Close the given resource.
	 * @param resource The resource to be closed.
	 */
	private static void close(Closeable resource) {
		if (resource != null) {
			try {
				resource.close();
			} catch (IOException ignore) {
				// Ignore IOException. If you want to handle this anyway, it might be useful to know
				// that this will generally only be thrown when the client aborted the request.
			}
		}
	}

	// Inner classes ------------------------------------------------------------------------------

	
	/**
	 * A sequence of paths; some of which may be in a zip file.
	 * There are multiple possibilities here
	 * <ol>
	 * <li>The application server unpacks the WAR and we have a file on the file system</li>
	 * <li>The application server unpacks the WAR and we have a file within a zip file on the file system</li>
	 * <li>The application server refuses to unpack the WAR and we have a file that get as a input stream (but in this we do not have the file length readily available)</li>
	 * <li>The application server refuses to unpack the WAR and we have a file within a input stream that is a zip file.</li>
	 * </ol>
	 * 
	 * All of these result in basically the same thing
	 * <ol>
	 * <li>A stream containing the requested content that can simply be written out to the servlet output stream.</li>
	 * <li>A length field that can be used as Content-Length</li>
	 * <li>A lastModified that can be used as in the ETag</li>
	 * <li></li>
	 * </ol> 
	 * @author mshankar
	 *
	 */
	private class PathSequence implements Closeable { 
		/**
		 * This is what the client is asking for.
		 */
		private String fullPathToResource;
		
		
		private BufferedInputStream content = null;
		private long length = -1;
		private long lastModified = -1;
		
		/**
		 * Used for debugging purposes; indicates how we served this content.
		 */
		private String identifier;
		
		private PathSequence(ServletContext servletContext, String basePath, String decodedPath) throws IOException {
			this.fullPathToResource = Paths.get(basePath, decodedPath).toString();
			logger.debug("Looking for resource " + fullPathToResource);
			
			// The application server unpacks the WAR and we have a file on the file system
			String pathOnDisk = servletContext.getRealPath(fullPathToResource);
			if(pathOnDisk != null) { 
				File f = new File(pathOnDisk);
				if(f.exists()) { 
					logger.debug("Found " + fullPathToResource + " on the file system here - " + pathOnDisk);
					this.length = f.length();
					this.lastModified = f.lastModified();
					if(templateReplacementPaths.contains(decodedPath)) { 
						templateReplace(decodedPath, new FileInputStream(f));
					} else { 
						this.content = new BufferedInputStream(new FileInputStream(f));
					}

					this.identifier = f.getAbsolutePath();
					return;
				}
			}
			URL pathURL = servletContext.getResource(fullPathToResource);
			if(pathURL != null) { 
				logger.debug("Found " + fullPathToResource + " as a URL here - " + pathURL.toString());
				URLConnection connection = pathURL.openConnection();
				this.length = connection.getContentLengthLong();
				this.lastModified = connection.getDate();
				if(templateReplacementPaths.contains(decodedPath)) { 
					templateReplace(decodedPath, connection.getInputStream());
				} else { 
					this.content = new BufferedInputStream(connection.getInputStream());
				}
				this.identifier = pathURL.toString();
				return;
			}
			
			Path pathSoFar = Paths.get(basePath);
			Path searchPath = Paths.get(decodedPath);
			int currentIndexIntoPath = 0;
			for(Path pathComponent : searchPath) {
				String potentialZipPath = pathSoFar.resolve(pathComponent.toString() + ".zip").toString();
				logger.debug("Checking to see if zip file " + potentialZipPath + " exists.");
				URL zipFileURL = servletContext.getResource(potentialZipPath);
				if(zipFileURL != null) { 
					logger.debug("Found zip file " + potentialZipPath + " at url " + zipFileURL);
					String potentialPathWithinZip = searchPath.subpath(currentIndexIntoPath+1, searchPath.getNameCount()).toString();
					logger.debug("Looking for '" + potentialPathWithinZip + "' within zip file " + zipFileURL.toString());
					URLConnection connection = zipFileURL.openConnection();
					try(ZipInputStream zis = new ZipInputStream(connection.getInputStream())) { 
						ZipEntry zentry = zis.getNextEntry();
						while(zentry != null) { 
							// logger.debug("Zip entry '" + zentry.getName() + "'");
							if(zentry.getName().equals(potentialPathWithinZip)) {
								this.length = zentry.getSize();
								this.lastModified = zentry.getTime();
								ByteArrayOutputStream bos = new ByteArrayOutputStream();
								byte[] buf = new byte[1024];
								int bytesRead = zis.read(buf);
								while(bytesRead > 0) { 
									bos.write(buf, 0, bytesRead);
									bytesRead = zis.read(buf);
								}
								logger.debug("Read bytes " + bos.size() + " for content length " + this.length);
								if(bos.size() != this.length) {
									throw new IOException("ZipEntry for " + potentialPathWithinZip + " in zip file " + zipFileURL.toString() + " says the content length is " + this.length + " but we could only read " + bos.size() + " bytes");
								}
								if(templateReplacementPaths.contains(decodedPath)) { 
									templateReplace(decodedPath, new ByteArrayInputStream(bos.toByteArray()));
								} else { 
									this.content = new BufferedInputStream(new ByteArrayInputStream(bos.toByteArray()));
								}
								this.identifier = zipFileURL.toString() + ".zip:" + potentialPathWithinZip;
								return;
							}
							zentry = zis.getNextEntry();
						}
					}
				}
				pathSoFar = pathSoFar.resolve(pathComponent.toString());
				currentIndexIntoPath++;
			}
		}

		
		public void templateReplace(String decodedPath, InputStream is) throws IOException {
			logger.debug("Template replacement for " + decodedPath.toString());
			
			switch(decodedPath) { 
			case "viewer/index.html": { 
				HashMap<String, String> templateReplacementsForViewer = new HashMap<String, String>();
				templateReplacementsForViewer.put("client_retrieval_url_base", 
						"<script>\n" 
				+ "window.global_options.retrieval_url_base = '" + configService.getMyApplianceInfo().getDataRetrievalURL() +  "';\n" 
				+ "</script>");
				ByteArrayInputStream replacedContent = SyncStaticContentHeadersFooters.templateReplaceChunksHTML(is, templateReplacementsForViewer);
				this.content = new BufferedInputStream(replacedContent);
				this.length = replacedContent.available();
				return;
			}
			case "js/mgmt.js": { 
				HashMap<String, String> templateReplacementsForViewer = new HashMap<String, String>();
				templateReplacementsForViewer.put("archivePVWorkflowBatchSize", 
						"var archivePVWorkflowBatchSize = " + configService.getMgmtRuntimeState().getArchivePVWorkflowBatchSize() +  ";\n");
				templateReplacementsForViewer.put("minimumSamplingPeriod", 
						"var minimumSamplingPeriod = " + configService.getInstallationProperties().getProperty("org.epics.archiverappliance.mgmt.bpl.ArchivePVAction.minimumSamplingPeriod", "0.1") +  ";\n");
				ByteArrayInputStream replacedContent = SyncStaticContentHeadersFooters.templateReplaceChunksJavascript(is, templateReplacementsForViewer);
				

				this.content = new BufferedInputStream(replacedContent);
				this.length = replacedContent.available();
				break;
			}
			default:
				logger.error("Template replacement for " + decodedPath.toString() + " that has been registered in error?");
			}
		}

		
		boolean exists() { 
			return this.content != null;
		}
		
		String getContentDispositionFileName() throws IOException {
			Path fullPath = Paths.get(fullPathToResource);
			int pathComponentsSz = fullPath.getNameCount();
			return fullPath.subpath(pathComponentsSz-1, pathComponentsSz).toString();
		}
		
		long length() { 
			return this.length;
		}
		
		long lastModified() {
			return this.lastModified;
		}
		
		InputStream getInputStream() throws IOException {
			return this.content;
		}
		
		@Override
		public String toString() {
			return this.identifier;
		}

		@Override
		public void close() throws IOException {
			if(this.content != null) { 
				try { this.content.close(); } catch (Throwable t) {} 
			}
		}
	}
}
