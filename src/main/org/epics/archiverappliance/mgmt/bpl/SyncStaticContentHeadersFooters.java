package org.epics.archiverappliance.mgmt.bpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
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
	
	public static class TextChunk {
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
	 * Regular text chunks come as REGULAR_TEXT chunks
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static LinkedList<TextChunk> breakFileIntoChunks(File file) throws IOException {
		String startOfChunkIndicator = "<!-- @begin(";
		String endOfChunkIndicator = "<!-- @end(";
		FileInputStream fis = new FileInputStream(file);
		return breakFileIntoChunks(fis, startOfChunkIndicator, endOfChunkIndicator);
	}
	
	
	/**
	 * Break down a file into a list of text chunks each of which is identified by the text string in the @begin(...) if any...
	 * Regular text chunks come as REGULAR_TEXT chunks
	 * This method closes the inputstream is after it is done.
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public static LinkedList<TextChunk> breakFileIntoChunks(InputStream is, String startOfChunkIndicator, String endOfChunkIndicator) throws IOException {
		LinkedList<TextChunk> textChunks = new LinkedList<TextChunk>();
		String currentChunkType = REGULAR_TEXT;
		StringBuffer currentTextChunk = new StringBuffer();
		try(LineNumberReader lineReader = new LineNumberReader(new InputStreamReader(is))) {
			String line = lineReader.readLine();
			while(line != null) {
				if(line.contains(startOfChunkIndicator)) {
					if(currentTextChunk.length() > 0) { 
						textChunks.add(new TextChunk(currentChunkType, currentTextChunk.toString()));
						currentTextChunk = new StringBuffer();
						currentChunkType = REGULAR_TEXT;
					}

					currentTextChunk.append(line);
					currentTextChunk.append("\n");
					int startOfChunkType = line.indexOf(startOfChunkIndicator) + startOfChunkIndicator.length();
					int endOfChunkType =  line.indexOf(')', startOfChunkType);
					currentChunkType = line.substring(startOfChunkType, endOfChunkType);					
				} else {
					if(line.contains(endOfChunkIndicator)) {
						currentTextChunk.append(line);
						currentTextChunk.append("\n");
						textChunks.add(new TextChunk(currentChunkType, currentTextChunk.toString()));
						currentTextChunk = new StringBuffer();
						currentChunkType = REGULAR_TEXT;
					} else {
						currentTextChunk.append(line);
						currentTextChunk.append("\n");
					}
				}
				line = lineReader.readLine();
			}
		}
		textChunks.add(new TextChunk(currentChunkType, currentTextChunk.toString()));
		
		return textChunks;
	}
	
	
	/**
	 * Breaks the HTML inputstream is into chunks and replaces the templates with the content in templateReplacements.
	 * The inputstream is is closed after this operation.
	 * @param is
	 * @param templateReplacements
	 * @return
	 * @throws IOException
	 */
	public static ByteArrayInputStream templateReplaceChunksHTML(InputStream is, HashMap<String, String> templateReplacements) throws IOException { 
		String startOfChunkIndicator = "<!-- @begin(";
		String endOfChunkIndicator = "<!-- @end(";
		String closingText = ") -->";
		return templateReplaceChunks(is, templateReplacements, startOfChunkIndicator, endOfChunkIndicator, closingText);
	}
	
	/**
	 * Breaks the Javascript inputstream is into chunks and replaces the templates with the content in templateReplacements.
	 * The inputstream is is closed after this operation.
	 * @param is
	 * @param templateReplacements
	 * @return
	 * @throws IOException
	 */
	public static ByteArrayInputStream templateReplaceChunksJavascript(InputStream is, HashMap<String, String> templateReplacements) throws IOException { 
		String startOfChunkIndicator = "// @begin(";
		String endOfChunkIndicator = "// @end(";
		String closingText = ")";
		return templateReplaceChunks(is, templateReplacements, startOfChunkIndicator, endOfChunkIndicator, closingText);
	}
	
	private static ByteArrayInputStream templateReplaceChunks(InputStream is, HashMap<String, String> templateReplacements, String startOfChunkIndicator, String endOfChunkIndicator, String closingText) throws IOException { 
		LinkedList<TextChunk> destTextChunks = SyncStaticContentHeadersFooters.breakFileIntoChunks(is, startOfChunkIndicator, endOfChunkIndicator);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try(PrintWriter out = new PrintWriter(new OutputStreamWriter(bos))) {
			for(TextChunk destTextChunk : destTextChunks) {
				if(destTextChunk.typeOfChunk.equals(REGULAR_TEXT)) {
					out.print(destTextChunk.textChunk);
				} else {
					String srcChunk = templateReplacements.get(destTextChunk.typeOfChunk);
					if(srcChunk == null) {
						out.print(destTextChunk.textChunk);
					} else {
						out.println(startOfChunkIndicator + destTextChunk.typeOfChunk + closingText);
						out.println(srcChunk);
						out.println(endOfChunkIndicator + destTextChunk.typeOfChunk + closingText);
					}
				}
			}
		}
		return new ByteArrayInputStream(bos.toByteArray());
	}
}
