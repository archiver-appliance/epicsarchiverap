package org.epics.archiverappliance.mgmt.bpl;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Small utility to sync the headers and footers from index.html to the other html files.
 * Typically called from the build script on demand.
 * This can also be used to do some simple site specific customization at startup time.  
 * <pre><code>Usage: java SyncStaticContentHeadersFooters &lt;IndexFileName&gt; &lt;Locations of static html files&gt;</code></pre>
 * @author mshankar
 *
 */
public class SyncStaticContentHeadersFooters {

	public static void main(String[] args) throws IOException {
		if(args.length < 2) {
			System.err.println("Usage: java SyncStaticContentHeadersFooters <IndexFileName> <Locations of other html files>");
			return;
		}
		String srcFileName = args[0];
		final File srcFile = new File(srcFileName);
		String[] fileLocations = Arrays.copyOfRange(args, 1, args.length);
		LinkedList<File> filesToModify = new LinkedList<File>();
		
		for(String fileLocation : fileLocations) {
			File fileLocationFile = new File(fileLocation);
			if(fileLocationFile.isDirectory()) {
				File[] htmlFiles = fileLocationFile.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						if(pathname.getName().endsWith(".html") && !pathname.equals(srcFile)) {
							return true;
						}
						return false;
					}
				});
				for(File htmlFile : htmlFiles) filesToModify.add(htmlFile);
			} else {
				filesToModify.add(fileLocationFile);
			}
		}
		
		
		LinkedList<TextChunk> srcTextChunks = breakFileIntoChunks(srcFile);
		HashMap<String, String> mainSrcFileTextChunks = new HashMap<String, String>();
		for(TextChunk textChunk : srcTextChunks) {
			if(!textChunk.typeOfChunk.equals(REGULAR_TEXT)) {
				mainSrcFileTextChunks.put(textChunk.typeOfChunk, textChunk.textChunk);
			}
		}
		
		for(File fileToModify : filesToModify) {
			System.out.println("Changing headers and footers for " + fileToModify.getAbsolutePath());
			File outputFile = new File(fileToModify.getAbsolutePath().concat("_generated"));
			try(PrintWriter out = new PrintWriter(new FileOutputStream(outputFile))) {
				LinkedList<TextChunk> destTextChunks = breakFileIntoChunks(fileToModify);
				for(TextChunk destTextChunk : destTextChunks) {
					if(destTextChunk.typeOfChunk.equals(REGULAR_TEXT)) {
						out.print(destTextChunk.textChunk);
					} else {
						String srcChunk = mainSrcFileTextChunks.get(destTextChunk.typeOfChunk);
						if(srcChunk == null) {
							out.print(destTextChunk.textChunk);
						} else {
							out.print(srcChunk);
						}
					}
				}
			}
			if(outputFile.exists() && outputFile.length() > 0) {
				Files.move(outputFile.toPath(), fileToModify.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
		}
	}
	
	static class TextChunk {
		String typeOfChunk;
		String textChunk;
		public TextChunk(String typeOfChunk, String textChunk) {
			this.typeOfChunk = typeOfChunk;
			this.textChunk = textChunk;
		}
	}
	
	private static final String REGULAR_TEXT = "RegularText";
	/**
	 * Break down a file into a list of text chunks each of which is identified by the text string in the @begin(...) if any...
	 * Regulat text chunks come as REGULAR_TEXT chunks
	 * @param file
	 * @return
	 * @throws IOException
	 */
	private static LinkedList<TextChunk> breakFileIntoChunks(File file) throws IOException {
		LinkedList<TextChunk> textChunks = new LinkedList<TextChunk>();
		String currentChunkType = REGULAR_TEXT;
		StringBuffer currentTextChunk = new StringBuffer();
		try(LineNumberReader lineReader = new LineNumberReader(new InputStreamReader(new FileInputStream(file)))) {
			String line = lineReader.readLine();
			while(line != null) {
				if(line.contains("<!-- @begin(")) {
					if(currentTextChunk.length() > 0) { 
						textChunks.add(new TextChunk(currentChunkType, currentTextChunk.toString()));
						currentTextChunk = new StringBuffer();
						currentChunkType = REGULAR_TEXT;
					}

					currentTextChunk.append(line);
					currentTextChunk.append("\n");
					int startOfChunkType = line.indexOf("<!-- @begin(") + "<!-- @begin(".length();
					int endOfChunkType =  line.indexOf(')', startOfChunkType);
					currentChunkType = line.substring(startOfChunkType, endOfChunkType);					
				} else if(line.contains("<!-- @end(")) {
					currentTextChunk.append(line);
					currentTextChunk.append("\n");
					textChunks.add(new TextChunk(currentChunkType, currentTextChunk.toString()));
					currentTextChunk = new StringBuffer();
					currentChunkType = REGULAR_TEXT;
				} else {
					currentTextChunk.append(line);
					currentTextChunk.append("\n");
				}
				line = lineReader.readLine();
			}
		}
		textChunks.add(new TextChunk(currentChunkType, currentTextChunk.toString()));
		
		return textChunks;
	}
}
