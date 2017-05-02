package org.epics.archiverappliance.utils.nio;


import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * This is a replacement for NIO Paths that caters to our syntax rules.
 * Normal file system paths are all similar to linux file names.
 * The compressed use the jar:file:syntax.
 * For example, <code>jar:file:///ziptest/alltext.zip!/SomeTextFile.txt</code>
 *  
 * @author luofeng
 * @author mshankar
 *
 */
public class ArchPaths implements Closeable {
	public static final String ZIP_PREFIX = "jar:file://";
	private static Logger logger = Logger.getLogger(ArchPaths.class.getName());
	private static FileSystemProvider zipFSProvider = getZipFSProvider();
	private ConcurrentHashMap<String, FileSystem>  fileSystemList = new ConcurrentHashMap<String, FileSystem>();	
	/**
	 * Return a path based on a varargs list of path components.
	 * Each path component is separated by the file separator.
	 * @param first  &emsp; 
	 * @param more  &emsp; 
	 * @return Path  &emsp; 
	 * @throws IOException  &emsp; 
	 */
	public Path get(String first, String... more) throws IOException {
		return get(false, first, more);
	}
	
	/**
	 * Return a path based on a varargs list of path components.
	 * Each path component is separated by the file separator.
	 * @param createParent If this is true, we create the parent automatically when requesting the path
	 * @param first  &emsp; 
	 * @param more  &emsp; 
	 * @return Path  &emsp; 
	 * @throws IOException &emsp; 
	 */
	public Path get(boolean createParent, String first, String... more) throws IOException {
		String path;
		if (more.length == 0) {
			path = first;
		} else {
			StringBuilder sb = new StringBuilder();
			sb.append(first);
			for (String segment: more) {
				if (segment.length() > 0) {
					if (sb.length() > 0) {
						sb.append('/');
					}
					sb.append(segment);
				}
			}
			path = sb.toString();
		}

		return this.get(path, createParent);
	}
	
	/**
	 * @param uriPathOrDefautFilePath  &emsp; 
	 * @return Path  &emsp; 
	 * @throws IOException  &emsp; 
	 */
	public Path get(String uriPathOrDefautFilePath) throws IOException {
		return get(uriPathOrDefautFilePath, false);
	}	
	/**
	 * Return a path based on the full URI representation of the path.
	 * 
	 * @param uriPathOrDefautFilePath  &emsp; 
	 * @param createParent If this is true, we create the parent automatically when requesting the path
	 * @return Path  &emsp; 
	 * @throws IOException  &emsp; 
	 */
	public Path get(String uriPathOrDefautFilePath, boolean createParent) throws IOException {
		if(uriPathOrDefautFilePath.startsWith(ZIP_PREFIX)){
			// We are dealing with zip files here.
			int sep=uriPathOrDefautFilePath.indexOf("!/");
			if (sep == -1){
				int sep2=uriPathOrDefautFilePath.indexOf(ZIP_PREFIX);
                if(sep2!=-1){
                        String defaultFile=uriPathOrDefautFilePath.substring(sep2+11);
                        return FileSystems.getDefault().getPath(defaultFile);   
                }else{
                	
                	 IOException e=new IOException("the uri path doesn't include the file path in zip fie. the url path should be like these:" +
                                        " jar:file:///D:/ziptest/alltext.zip!/SomeTextFile.txt or jar:file:///ziptest/alltext.zip!/SomeTextFile.txt,or jar:file:///ziptest/, but your path is "+uriPathOrDefautFilePath);       
                     logger.error("the uri path doesn't include the file path in zip fie. the url path should be like these:" +
                                        " jar:file:///D:/ziptest/alltext.zip!/SomeTextFile.txt or jar:file:///ziptest/alltext.zip!/SomeTextFile.txt,or jar:file:///ziptest/, but your path is "+uriPathOrDefautFilePath,e);
                     throw e;
                }

			}
			if(logger.isDebugEnabled()) logger.debug("Asking for " + uriPathOrDefautFilePath + (createParent ? " with and option to create parent folder" : " without the option to create the parent folder"));
			String zipPathStr = uriPathOrDefautFilePath.substring(ZIP_PREFIX.length(), sep);
			if(logger.isDebugEnabled()) logger.debug("The path to the zip file is " + zipPathStr);
			String innerFilePath = uriPathOrDefautFilePath.substring(sep+1);
			if(logger.isDebugEnabled()) logger.debug("The path to the file within the zip file is " + innerFilePath);
			
			FileSystem zipfs = null;
			if(fileSystemList.get(zipPathStr) != null) {
				logger.debug("We already have the zip file open in this context " + zipPathStr);
				zipfs = fileSystemList.get(zipPathStr);
			} else {
				Path zipPath = FileSystems.getDefault().getPath(zipPathStr);
				if(!Files.exists(zipPath)) {
					if(createParent) {
						Files.createDirectories(zipPath.getParent());
						logger.debug("Creating the zip file.");
						Map<String, String> env = new HashMap<>();
						env.put("create", "true");
						zipfs = zipFSProvider.newFileSystem(zipPath, env); 
						if(zipfs == null) throw new IOException("Unable to get a new file system from the provider.");
						fileSystemList.put(zipPathStr, zipfs);
					} else {
						throw new NoSuchFileException("The zip file " + zipPathStr + " does not exist and we do not have the createParent set to true");
					}
				} else {
					Map<String, String> env = new HashMap<>();
					env.put("create", "false");
					zipfs = zipFSProvider.newFileSystem(zipPath, env); 
					if(zipfs == null) throw new IOException("Unable to get a new file system from the provider.");
					fileSystemList.put(zipPathStr, zipfs);
				}
			}
			
			Path pathWithinZipFile = zipfs.getPath(innerFilePath);
			Path parent = pathWithinZipFile.getParent();
			if (createParent && parent != null && Files.notExists(parent)) {
				if(logger.isDebugEnabled()) logger.debug("Creating parent folder " + parent.toString());
				Files.createDirectories(parent);
			}
			return pathWithinZipFile;
		} else {
			if(uriPathOrDefautFilePath.contains(":") && uriPathOrDefautFilePath.indexOf(":") < uriPathOrDefautFilePath.indexOf("/") && !uriPathOrDefautFilePath.startsWith("file:///")) {
				try { 
					URI uri = new URI(uriPathOrDefautFilePath);
					FileSystem fs = FileSystems.newFileSystem(uri, System.getenv(), Thread.currentThread().getContextClassLoader());
					Path normalFilePath = fs.getPath(uri.getPath());
					Path parent = normalFilePath.getParent();
					if (createParent && parent != null && Files.notExists(parent)) {
						if(logger.isDebugEnabled()) logger.debug("Creating parent folder " + parent.toString());
						Files.createDirectories(parent);
					}
		
					return normalFilePath;
				} catch(URISyntaxException ex) { 
					throw new IOException(ex);
				}
				
			} else { 
				// We are dealing with normal file system paths.
				Path normalFilePath = FileSystems.getDefault().getPath(uriPathOrDefautFilePath);
				Path parent = normalFilePath.getParent();
				if (createParent && parent != null && Files.notExists(parent)) {
					if(logger.isDebugEnabled()) logger.debug("Creating parent folder " + parent.toString());
					Files.createDirectories(parent);
				}
	
				return normalFilePath;
			}
		}
	}
	
	
	/**
	 * Returns a seekable byte channel. 
	 * In case of file systems, this is the raw SeekableByteChannel as returned by the provider.
	 * In case of zip files, we wrap the InputStream using WrappedSeekableByteChannel (which is a read only byte channel for now).
	 * @param path Path 
	 * @param options  OpenOption
	 * @return a new seekabel byte channel 
	 * @throws IOException  &emsp; 
	 */
	public static SeekableByteChannel newByteChannel(Path path, OpenOption...options) throws IOException {
		String pathURI = path.toUri().toString();
		if(pathURI.startsWith(ZIP_PREFIX)) {
			return new WrappedSeekableByteChannel(path);
		} else {
			return Files.newByteChannel(path, options);
		}
	}
	

	private static FileSystemProvider getZipFSProvider() {
		for (FileSystemProvider provider : FileSystemProvider.installedProviders()) {
			if ("jar".equals(provider.getScheme()))
				return provider;
		}
		logger.fatal("For some reason we do not have the zip file system provider on this JVM.");
		return null;
	}

	@Override
	public void close() throws IOException {
		for(String key:fileSystemList.keySet()){
			if(logger.isDebugEnabled()) logger.debug("Closing file system for " + key);
			fileSystemList.get(key).close();
		}
	}
	
	/**
	 * The path used for backing up the data using ETL.
	 * When enabling compression with packing, we backup the packed file itself as opposed to the files within the packed file.
	 * @param path Path 
	 * @return Path for backup 
	 * @throws IOException  &emsp; 
	 */
	public String getPathForBackup(Path path) throws IOException {
		String uriStr = path.toUri().toString();
		if(uriStr.startsWith(ZIP_PREFIX)) {
			int sep = uriStr.indexOf("!/");
			if(sep == -1) {
				throw new IOException("Malformed URI :" + uriStr);
			}

			String zipPathStr = uriStr.substring(ZIP_PREFIX.length(), sep);
			return zipPathStr;
		} else {
			// For regular file systems, the container path is the same as the regular path
			return path.toFile().getAbsolutePath();
		}
	}
}

