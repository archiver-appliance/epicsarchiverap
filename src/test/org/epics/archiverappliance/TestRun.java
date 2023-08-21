package org.epics.archiverappliance;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class TestRun {

    public static void main(String[] args) throws Exception {

        var options = new Options()
                .addOption("t", "testname", true, "Test name")
                .addOption("c", "clusters", true, "Number of clusters");

        var parser = new DefaultParser();

        try {
            var cmdLine = parser.parse(options, args);
            if (cmdLine.hasOption('c')) {
                System.err.println("Running a cluster");
            }

            int clusters = Integer.parseInt(cmdLine.getOptionValue("c", "1"));
            var testName = cmdLine.getOptionValue("t", "./bin");
            var tomcat = new TomcatSetup();
            try {
                if (clusters > 1) {
                    tomcat.setUpClusterWithWebApps(testName, clusters);
                } else {
                    tomcat.setUpWebApps(testName);
                }
            } catch (InterruptedException ex) {
                tomcat.tearDown();
                throw ex;
            }
        } catch (ParseException e) {
            e.printStackTrace();
            new HelpFormatter().printHelp("args...", options);
        }
    }
}
