package org.epics.archiverappliance.mgmt.appxml;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetAddress;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * Test various versions of appliances.xml and make sure we can start the config service correctly.
 * @author mshankar
 *
 */
@Tag("integration")
public class FQDNApplianceXMLTest {
	private static Logger logger = LogManager.getLogger(FQDNApplianceXMLTest.class.getName());
	File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ApplianceXMLTest");
	TomcatSetup tomcatSetup = new TomcatSetup();
	WebDriver driver;

	@BeforeAll
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@BeforeEach
	public void setUp() throws Exception {
		if(testFolder.exists()) { 
			FileUtils.deleteDirectory(testFolder);
		}
		testFolder.mkdirs();		
	}

	@AfterEach
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(testFolder);
	}


	@Test
	public void testFQDNAppliancesXML() throws Exception {
		String fqdn = InetAddress.getLocalHost().getCanonicalHostName();
		logger.info("Testing appliances.xml with FQDN using " + fqdn);
		String appliancesFilename = testFolder.getAbsolutePath() + File.separator + "fqdn_appliances.xml";
		try(PrintWriter out = new PrintWriter(new File(appliancesFilename))) { 
			out.println("<appliances>\n\t<appliance>\n\t\t<identity>appliance0</identity>\n\t\t" + 
					"<cluster_inetport>" + fqdn + ":16670</cluster_inetport>\n\t\t<mgmt_url>http://" + fqdn + ":17665/mgmt/bpl</mgmt_url>\n\t\t" + 
					"<engine_url>http://" + fqdn + ":17665/engine/bpl</engine_url>\n\t\t<etl_url>http://" + fqdn + ":17665/etl/bpl</etl_url>" + 
					"<retrieval_url>http://" + fqdn + ":17665/retrieval/bpl</retrieval_url>\n\t\t<data_retrieval_url>http://" + fqdn + ":17665/retrieval</data_retrieval_url>\n\t" +  
					"</appliance>\n\t</appliances>");
		}
		System.getProperties().put(ConfigService.ARCHAPPL_APPLIANCES, appliancesFilename);

		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();
		
		driver.get("http://localhost:17665/mgmt/ui/index.html");
		WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		Assertions.assertTrue(pvstextarea != null, "Cannot get to the home page...");

		driver.quit();
		tomcatSetup.tearDown();
	}	
}
