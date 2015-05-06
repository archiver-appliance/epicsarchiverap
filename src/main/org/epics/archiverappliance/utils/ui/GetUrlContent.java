/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.ui;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Small utility for getting the contents of an URL as various things
 * @author mshankar
 * 
 */
/**
 * @author mshankar
 *
 */
public class GetUrlContent {
	public static final String ARCHAPPL_COMPONENT = "ARCHAPPL_COMPONENT";
	private static final Logger logger = Logger.getLogger(GetUrlContent.class);
	
	/**
	 * Small utility method for getting the content of an URL as a string
	 * Returns null in case of an exception.
	 * @param urlStr
	 * @return
	 */
	public static String getURLContent(String urlStr) {
		try {
			logger.debug("Getting the contents of " + urlStr + " as a string.");
			try (InputStream is = getURLContentAsStream(urlStr)) {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				IOUtils.copy(is, bos);
				bos.close();
				return new String(bos.toByteArray(), "UTF-8");
			}
		} catch (IOException ex) {
			logger.error("Exception getting contents of internal URL " + urlStr, ex);
		}
		return null;
	}
	
	/**
	 * Given a URL, get the contents as a JSON Array
	 * @param urlStr
	 * @return
	 */
	public static JSONArray getURLContentAsJSONArray(String urlStr) {
		try {
			logger.debug("Getting the contents of " + urlStr + " as a JSON array.");
			JSONParser parser=new JSONParser();
			try (InputStream is = getURLContentAsStream(urlStr)) {
				return (JSONArray) parser.parse(new InputStreamReader(is));
			}
		} catch (IOException ex) {
			logger.error("Exception getting contents of internal URL " + urlStr, ex);
		} catch (ParseException pex) {
			logger.error("Parse exception getting contents of internal URL " + urlStr + " at " + pex.getPosition(), pex);
		}
		return null;
	}
	
	/**
	 * Given an URL, get the contents as a JSON Object
	 * @param urlStr
	 * @return
	 */
	public static JSONObject getURLContentAsJSONObject(String urlStr) {
		return getURLContentAsJSONObject(urlStr, true);
	}
	
	/**
	 * Given an URL, get the contents as a JSON Object; control logging.
	 * @param urlStr
	 * @param logErrors - if false, do not log any exceptions (they are expected)
	 * @return
	 */
	public static JSONObject getURLContentAsJSONObject(String urlStr, boolean logErrors) {
		try {
			logger.debug("Getting the contents of " + urlStr + " as a JSON object.");
			JSONParser parser=new JSONParser();
			try (InputStream is = getURLContentAsStream(urlStr)) {
				return (JSONObject) parser.parse(new InputStreamReader(is));
			}
		} catch (IOException ex) {
			if(logErrors) logger.error("Exception getting contents of internal URL " + urlStr, ex);
		} catch (ParseException pex) {
			if(logErrors) logger.error("Parse exception getting contents of internal URL " + urlStr + " at " + pex.getPosition(), pex);
		}
		return null;
	}
	
	/**
	 * Combine JSON arrays from multiple URL's in sequence and return a JSON Array.
	 * We need the supress warnings here as JSONArray is a raw collection.
	 * 
	 * @param urls
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static JSONArray combineJSONArrays(List<String> urlStrs) {
		JSONArray result = new JSONArray();
		for(String urlStr : urlStrs) {
			try {
				logger.debug("Getting the contents of " + urlStr + " as a JSON array.");
				JSONParser parser=new JSONParser();
				try (InputStream is = getURLContentAsStream(urlStr)) {
					JSONArray content = (JSONArray) parser.parse(new InputStreamReader(is));
					if(content != null) {
						result.addAll(content);
					} else {
						logger.debug(urlStr + " returned an empty array");
					}
				}
			} catch (IOException ex) {
				logger.error("Exception getting contents of internal URL " + urlStr, ex);
			} catch (ParseException pex) {
				logger.error("Parse exception getting contents of internal URL " + urlStr + " at " + pex.getPosition(), pex);
			}
		}
		return result;
	}
	
	/**
	 * Combine JSON arrays of JSON objects from multiple URL's in sequence and sends them to the writer..
	 * The difference from combineJSONArrays is that inserts a newline after each element.
	 * 
	 * @param urls
	 * @return
	 */
	public static void combineJSONArraysAndPrintln(List<String> urlStrs, PrintWriter out) {
		out.println("[");
		boolean first = true;
		for(String urlStr : urlStrs) {
			try {
				logger.debug("Getting the contents of " + urlStr + " as a JSON array.");
				JSONParser parser=new JSONParser();
				try (InputStream is = getURLContentAsStream(urlStr)) {
					JSONArray content = (JSONArray) parser.parse(new InputStreamReader(is));
					if(content != null) {
						for(Object obj : content) {
							JSONObject jsonObj = (JSONObject) obj;
							if(first) { first = false; } else { out.println(","); } 
							out.print(JSONValue.toJSONString(jsonObj));
						}
					} else {
						logger.debug(urlStr + " returned an empty array");
					}
				}
			} catch (IOException ex) {
				logger.error("Exception getting contents of internal URL " + urlStr, ex);
			} catch (ParseException pex) {
				logger.error("Parse exception getting contents of internal URL " + urlStr + " at " + pex.getPosition(), pex);
			}
		}
		out.println("]");
	}
	
	/**
	 * A static utilty method to combine JSON objects
	 * @param dest Details from additionalDetails are added to this. 
	 * @param additionalDetails
	 */
	@SuppressWarnings("unchecked")
	public static void combineJSONObjects(HashMap<String, String> dest, JSONObject additionalDetails) {
		if(additionalDetails != null) dest.putAll(additionalDetails);
	}
	
	/**
	 * A static utilty method to combine JSON objects
	 * @param dest Details from additionalDetails are added to this.
	 * @param additionalDetails
	 */
	@SuppressWarnings("unchecked")
	public static void combineJSONArrays(LinkedList<Map<String, String>> dest, JSONArray additionalDetails) {
		if(additionalDetails != null) dest.addAll(additionalDetails);
	}
	
	
	@SuppressWarnings("unchecked")
	public static void combineJSONObjectsWithArrays(HashMap<String, Object> dest, JSONObject additionalDetails) {
		if(additionalDetails != null) dest.putAll(additionalDetails);
	}
	
	
	
	/**
	 * Post a JSONArray to a remote server and get the response as a JSON object.
	 * @param url
	 * @param array
	 * @return
	 * @throws IOException
	 */
	public static JSONObject postDataAndGetContentAsJSONObject(String url, LinkedList<JSONObject> array) throws IOException {
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost postMethod = new HttpPost(url);
		postMethod.addHeader(ARCHAPPL_COMPONENT, "true");
		postMethod.addHeader("Content-Type", MimeTypeConstants.APPLICATION_JSON);
		StringEntity archiverValues = new StringEntity(JSONValue.toJSONString(array), ContentType.APPLICATION_JSON);
		postMethod.setEntity(archiverValues);
		if(logger.isDebugEnabled()) {
			logger.debug("About to make a POST with " + url);
		}
		HttpResponse response = httpclient.execute(postMethod);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
			// ArchiverValuesHandler takes over the burden of closing the input stream.
			try(InputStream is = entity.getContent()) {
				JSONObject retval = (JSONObject) JSONValue.parse(new InputStreamReader(is));
				return retval;
			}
		} else {
			throw new IOException("HTTP response did not have an entity associated with it");
		}
	}
	
	
	/**
	 * Post a JSONObject to a remote server and get the response as a JSON object.
	 * @param url
	 * @param object
	 * @return
	 * @throws IOException
	 */
	public static JSONObject postObjectAndGetContentAsJSONObject(String url, JSONObject object) throws IOException {
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPost postMethod = new HttpPost(url);
		postMethod.addHeader(ARCHAPPL_COMPONENT, "true");
		postMethod.addHeader("Content-Type", MimeTypeConstants.APPLICATION_JSON);
		StringEntity archiverValues = new StringEntity(JSONValue.toJSONString(object), ContentType.APPLICATION_JSON);
		postMethod.setEntity(archiverValues);
		if(logger.isDebugEnabled()) {
			logger.debug("About to make a POST with " + url);
		}
		HttpResponse response = httpclient.execute(postMethod);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
			// ArchiverValuesHandler takes over the burden of closing the input stream.
			try(InputStream is = entity.getContent()) {
				JSONObject retval = (JSONObject) JSONValue.parse(new InputStreamReader(is));
				return retval;
			}
		} else {
			throw new IOException("HTTP response did not have an entity associated with it");
		}
	}

	
	/**
	 * Check if we get a valid response from this URL
	 * @param urlStr
	 * @return
	 * @throws IOException
	 */
	public static boolean checkURL(String urlStr) {
		try {
			logger.debug("Testing if " + urlStr + " is valid");
			try (InputStream is = getURLContentAsStream(urlStr)) {
				return true;
			}
		} catch (IOException ex) {
			// Ignore any exceptions here as we are only testing if this is a valid URL.
		}
		return false;
	}
	
	
	private static InputStream getURLContentAsStream(String serverURL) throws IOException {
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpGet getMethod = new HttpGet(serverURL);
		getMethod.addHeader(ARCHAPPL_COMPONENT, "true");
		HttpResponse response = httpclient.execute(getMethod);
		if(response.getStatusLine().getStatusCode() == 200) {
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				logger.debug("Obtained a HTTP entity of length " + entity.getContentLength());
				// ArchiverValuesHandler takes over the burden of closing the input stream.
				InputStream is = entity.getContent();
				return is;
			} else {
				throw new IOException("HTTP response did not have an entity associated with it");
			}
		} else {
			throw new IOException("Invalid status calling " + serverURL + ". Got " + response.getStatusLine().getStatusCode() + response.getStatusLine().getReasonPhrase());
		}
	}
	
	
	
	/**
	 * Get the contents of a redirect URL and use as reponse for the provided HttpServletResponse.
	 * If possible, pass in error responses as well.
	 * @param redirectURIStr
	 * @param resp
	 * @throws IOException
	 */
	public static void proxyURL(String redirectURIStr, HttpServletResponse resp) throws IOException { 
		// We'll use java.net for now.
		HttpURLConnection.setFollowRedirects(true);
		URL url = new URL(redirectURIStr);
		HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
		if(urlConnection.getResponseCode() == 200) {
			try(OutputStream os = resp.getOutputStream(); InputStream is = new BufferedInputStream(urlConnection.getInputStream())) {
				byte buf[] = new byte[10*1024];
				int bytesRead = is.read(buf);
				while(bytesRead > 0) {
					os.write(buf, 0, bytesRead);
					bytesRead = is.read(buf);
				}
			}
		} else {
			logger.error("Invalid status code " + urlConnection.getResponseCode() + " when connecting to URL " + redirectURIStr + ". Sending the errorstream across");
			try(OutputStream os = resp.getOutputStream(); InputStream is = new BufferedInputStream(urlConnection.getErrorStream())) {
				byte buf[] = new byte[10*1024];
				int bytesRead = is.read(buf);
				while(bytesRead > 0) {
					os.write(buf, 0, bytesRead);
					bytesRead = is.read(buf);
				}
			}
			resp.sendError(urlConnection.getResponseCode());
		}

	}
}
