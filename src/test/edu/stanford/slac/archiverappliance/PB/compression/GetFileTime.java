package edu.stanford.slac.archiverappliance.PB.compression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.InputStream;

public class GetFileTime {
    private static final Logger logger = LogManager.getLogger(GetFileTime.class.getName());

    /**
     * @param args
     */
    public static void main(String[] args) {
        //
        GetFileTime.getTimeOnePVForOneDayFromZipPBFileincluding200000pvsOneday();
    }

    public static void getTimeOnePVForOneDayFromZipPBFileincluding200000pvsOneday() {
        long time1 = System.currentTimeMillis();
        try {
            // search files for one week from 2012_06_01 to 2012_06_07

            File zipFile = new File("/scratch/200000pvsforoneday/result/2012_01_01.zip");
            InputStream in = GZIPUtil.unpackZip(zipFile, "pv100000:2012_01_01.pb.gz");
            // String filename=
            GZIPUtil.unGZFile(in, "/scratch/200000pvsforoneday/result/", "pv100000:2012_01_01.pb");
            // System.out.println(filename);
            // GZIPUtil.unziptarFile(in, "pv54:2012_03_31_11.pb", "/scratch/", "new_pv54:2012_03_31_11.pb");
            long time2 = System.currentTimeMillis();
            System.out.println("time consumed is :" + (time2 - time1) + "ms");
        } catch (Exception e) {
            logger.error("exception", e);
        }
    }
}
