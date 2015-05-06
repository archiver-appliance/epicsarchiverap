package edu.stanford.slac.archiverappliance.PB.compression.zipfs;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
public class ArchPathsTest extends TestCase {
	private String rootFolderStr = ConfigServiceForTests.getDefaultPBTestFolder() + "/ArchPathsTest";
	private static Logger logger = Logger.getLogger(ArchPathsTest.class.getName());
	
	@Before
	public void setUp() throws Exception {
		File rootFolder = new File(rootFolderStr);
		FileUtils.deleteDirectory(rootFolder);
		rootFolder.mkdirs();
		// We create some sample files for testing.
		for(int filenum = 1; filenum < 100; filenum++) {
			try(PrintWriter out = new PrintWriter(new FileOutputStream(new File(rootFolderStr + File.separator + "text"  + filenum + ".txt")))) {
				for(int i = 0; i < 1000; i++) {
					out.println("Line " + i);
				}
			}
		}
	}

	@After
	public void tearDown() throws Exception {
		// FileUtils.deleteDirectory(new File(rootFolderStr));
	}

	@Test
	public void testPack() throws Exception {
		String zipFileName = rootFolderStr + "/pack.zip";
		String zipFilePath = "jar:file://" + zipFileName + "!/result/SomeTextFile.txt";
		try(ArchPaths arch = new ArchPaths()) {
			Path sourcePath = arch.get(rootFolderStr + "/text1.txt");
			Path destPath = arch.get(zipFilePath, true);
			Files.copy(sourcePath, destPath, REPLACE_EXISTING);
		}
		
		assertTrue("Zip file does not exist " + zipFileName, new File(zipFileName).exists());

		try(ArchPaths validateArchPaths = new ArchPaths()) {
			assertTrue("Path does not exist after packing " + zipFilePath, Files.exists(validateArchPaths.get(zipFilePath)));
		}
		
		// Now we test adding more files.
		try(ArchPaths arch=	new ArchPaths()) {
			for(int filenum = 2; filenum < 10; filenum++) {
				Path sourcePath = arch.get(rootFolderStr + "/text" + filenum + ".txt");
				Path destPath = arch.get("jar:file://" + zipFileName + "!/result/SomeTextFile" + filenum + ".txt");
				logger.debug("Packing " + sourcePath.toString() + " into " + destPath.toString());
				Files.copy(sourcePath, destPath, REPLACE_EXISTING);
			}
		}
		
		try(ArchPaths validateArchPaths = new ArchPaths()) {
			for(int filenum = 2; filenum < 10; filenum++) {
				assertTrue("Path does not exist after packing " + zipFilePath, Files.exists(validateArchPaths.get("jar:file:///" + zipFileName + "!/result/SomeTextFile" + filenum + ".txt")));
			}
		}
	}	
	
	
	@Test
	public void testConcurrentAccess() throws Exception {
		// Test to see if we can access the zip file concurrently. 
		// We launch two threads and see if one can add while the other can read
		
		final String zipFileName = rootFolderStr + "/concurrpack.zip";
		Runnable writer = new Runnable() {
			@Override
			public void run() {
				int exceptionCount = 0;
				try {
					for(int filenum = 1; filenum < 100; filenum++) {
						try(ArchPaths paths = new ArchPaths()) {
							Path sourcePath = paths.get(rootFolderStr + "/text" + filenum + ".txt");
							Path destPath = paths.get("jar:file://" + zipFileName + "!/result/SomeTextFile" + filenum + ".txt", true);
							logger.debug("Packing " + sourcePath.toString() + " into " + destPath.toString());
							Files.copy(sourcePath, destPath, REPLACE_EXISTING);
						} catch(Exception ex) {
							exceptionCount++;
							logger.error("Exception writing file", ex);
						}
						Thread.sleep(10);
					}
				} catch(Exception ex) {
					logger.error(ex);
				}
				assertTrue("The write thread had " + exceptionCount + " exceptions", exceptionCount == 0);
			}
		};
		final Thread writerThread = new Thread(writer);
		writerThread.setName("Writer");
		writerThread.start();
		
		Runnable reader = new Runnable() {
			@Override
			public void run() {
				int exceptionCount = 0;
				boolean checkedAtLeastOnce = false;
				while(writerThread.isAlive()) {
					if(new File(zipFileName).exists()) {
						logger.debug("Checking concurrent access");
						try {
							for(int filenum = 1; filenum < 100; filenum++) {
								try(ArchPaths paths = new ArchPaths()) {
									checkedAtLeastOnce = true;
									Path destPath = paths.get("jar:file://" + zipFileName + "!/result/SomeTextFile" + filenum + ".txt");
									Files.exists(destPath);
								} catch(Exception ex) {
									exceptionCount++;
									logger.error("Exception reading file", ex);
								}
							}
							Thread.sleep(10);
						} catch(Exception ex) {
							logger.error(ex);
						}
					} else {
						logger.debug("Skipping checking concurrent access");
					}
				}
				assertTrue("We have not check the reader part even once ", checkedAtLeastOnce);
				assertTrue("The read thread had " + exceptionCount + " exceptions", exceptionCount == 0);
			}
		};

		Thread readerthread = new Thread(reader);
		readerthread.setName("Reader");
		readerthread.start();
	}
}
