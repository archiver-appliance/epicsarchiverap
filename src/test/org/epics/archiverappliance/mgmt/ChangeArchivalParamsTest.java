package org.epics.archiverappliance.mgmt;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.TomcatSetup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import java.util.List;

@Tag("integration")
@Tag("localEpics")
public class ChangeArchivalParamsTest {
    private static Logger logger = LogManager.getLogger(ChangeArchivalParamsTest.class.getName());
    TomcatSetup tomcatSetup = new TomcatSetup();
    SIOCSetup siocSetup = new SIOCSetup();
    WebDriver driver;

    @BeforeAll
    public static void setupClass() {
        WebDriverManager.firefoxdriver().setup();
    }

    @BeforeEach
    public void setUp() throws Exception {
        siocSetup.startSIOCWithDefaultDB();
        tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
        driver = new FirefoxDriver();
    }

    @AfterEach
    public void tearDown() throws Exception {
        driver.quit();
        tomcatSetup.tearDown();
        siocSetup.stopSIOC();
    }

    @Test
    public void testChangeArchivalParams() throws Exception {
        driver.get("http://localhost:17665/mgmt/ui/index.html");
        WebElement pvstextarea = driver.findElement(By.id("archstatpVNames"));
        String pvNameToArchive = "UnitTestNoNamingConvention:sine";
        pvstextarea.sendKeys(pvNameToArchive);
        WebElement archiveButton = driver.findElement(By.id("archstatArchive"));
        logger.info("About to submit");
        archiveButton.click();
        Thread.sleep(10 * 1000);
        WebElement checkStatusButton = driver.findElement(By.id("archstatCheckStatus"));
        checkStatusButton.click();
        // We have to wait for a few minutes here as it does take a while for the workflow to complete.
        Thread.sleep(5 * 60 * 1000);
        driver.get("http://localhost:17665/mgmt/ui/pvdetails.html?pv=" + pvNameToArchive);
        Thread.sleep(2 * 1000);
        WebElement changePVParams = driver.findElement(By.id("pvDetailsParamChange"));
        logger.info("About to start dialog");
        changePVParams.click();
        WebElement samplingPeriodTextBox = driver.findElement(By.id("pvDetailsSamplingPeriod"));
        samplingPeriodTextBox.clear();
        samplingPeriodTextBox.sendKeys("11"); // A sample every 11 seconds
        WebElement dialogOkButton = driver.findElement(By.id("pvDetailsParamsOk"));
        logger.info("About to submit");
        dialogOkButton.click();
        Thread.sleep(10 * 1000);
        WebElement pvDetailsTable = driver.findElement(By.id("pvDetailsTable"));
        List<WebElement> pvDetailsTableRows = pvDetailsTable.findElements(By.cssSelector("tbody tr"));
        for (WebElement pvDetailsTableRow : pvDetailsTableRows) {
            WebElement pvDetailsTableFirstCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(1)"));
            if (pvDetailsTableFirstCol.getText().contains("Sampling period")) {
                WebElement pvDetailsTableSecondCol = pvDetailsTableRow.findElement(By.cssSelector("td:nth-child(2)"));
                String obtainedSamplingPeriod = pvDetailsTableSecondCol.getText();
                String expectedSamplingPeriod = "11.0";
                Assertions.assertTrue(
                        expectedSamplingPeriod.equals(obtainedSamplingPeriod),
                        "Expecting sampling period to be " + expectedSamplingPeriod + "; instead it is "
                                + obtainedSamplingPeriod);
                break;
            }
        }
    }
}
