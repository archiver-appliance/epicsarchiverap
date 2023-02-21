package org.epics.archiverappliance.mgmt.appxml;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetAddress;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

/**
 * Test various versions of appliances.xml and make sure we can start the config service correctly.
 * @author mshankar
 *
 */
@Category(IntegrationTests.class)
public class IPApplianceXMLTest {
	private static Logger logger = Logger.getLogger(IPApplianceXMLTest.class.getName());
	File testFolder = new File(ConfigServiceForTests.getDefaultPBTestFolder() + File.separator + "ApplianceXMLTest");
	TomcatSetup tomcatSetup = new TomcatSetup();
	WebDriver driver;

	@BeforeClass
	public static void setupClass() {
		WebDriverManager.firefoxdriver().setup();
	}

	@Before
	public void setUp() throws Exception {
		if(testFolder.exists()) { 
			FileUtils.deleteDirectory(testFolder);
		}
		testFolder.mkdirs();		
	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(testFolder);
	}


	@Test
	public void testIPAddressAppliancesXML() throws Exception {
		String ipaddress = InetAddress.getLocalHost().getHostAddress();
		logger.info("Testing appliances.xml with ip address using " + ipaddress);
		String appliancesFilename = testFolder.getAbsolutePath() + File.separator + "ipaddress_appliances.xml";
		try(PrintWriter out = new PrintWriter(new File(appliancesFilename))) { 
			out.println("<appliances>\n\t<appliance>\n\t\t<identity>appliance0</identity>\n\t\t" + 
					"<cluster_inetport>" + ipaddress + ":16670</cluster_inetport>\n\t\t<mgmt_url>http://" + ipaddress + ":17665/mgmt/bpl</mgmt_url>\n\t\t" + 
					"<engine_url>http://" + ipaddress + ":17665/engine/bpl</engine_url>\n\t\t<etl_url>http://" + ipaddress + ":17665/etl/bpl</etl_url>" + 
					"<retrieval_url>http://" + ipaddress + ":17665/retrieval/bpl</retrieval_url>\n\t\t<data_retrieval_url>http://" + ipaddress + ":17665/retrieval</data_retrieval_url>\n\t" +  
					"</appliance>\n\t</appliances>");
		}
		System.getProperties().put(ConfigService.ARCHAPPL_APPLIANCES, appliancesFilename);
		

		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
		driver = new FirefoxDriver();

		driver.get("http://localhost:17665/mgmt/ui/index.html");
		WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
		assertTrue("Cannot get to the home page...", pvstextarea != null);
		
		driver.quit();
		tomcatSetup.tearDown();
	}

}
