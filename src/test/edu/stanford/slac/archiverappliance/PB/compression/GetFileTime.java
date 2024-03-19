package edu.stanford.slac.archiverappliance.PB.compression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin.pbFileExtension;

public class GetFileTime {
	private static Logger logger = LogManager.getLogger(GetFileTime.class.getName());
        /**
         * @param args
         */
        public static void main(String[] args) {
                // 
                GetFileTime.getTimeOnePVForOneDayFromZipPBFileincluding200000pvsOneday();
        }
        
        
        public static void getTimeOnePVForOneDayFromZipPBFileincluding200000pvsOneday()
        {
     long time1=System.currentTimeMillis();
                try {
                        // search files for one week from 2012_06_01 to 2012_06_07
                        
                        File zipFile =new File("/scratch/200000pvsforoneday/result/2012_01_01.zip");
                        InputStream in=GZIPUtil.unpackZip(zipFile,"pv100000:2012_01_01.pb.gz");
                        //String filename=
                                        GZIPUtil.unGZFile(in, "/scratch/200000pvsforoneday/result/", "pv100000:2012_01_01.pb");
                        //System.out.println(filename);
                        //GZIPUtil.unziptarFile(in, "pv54:2012_03_31_11.pb", "/scratch/", "new_pv54:2012_03_31_11.pb");
                        long time2=System.currentTimeMillis();
                        System.out.println("time consumed is :"+(time2-time1)+"ms");
                } catch (IOException e) {
                    logger.error("exception", e);
                } catch (Exception e) {
                    logger.error("exception", e);
                }
        }
        
        public static void getTimeOnePVForOneWeekFrom7ZipPBFileincluding5000pvsOneday()
        {
                long time1=System.currentTimeMillis();
                
                try {
                        // search files for one week from 2012_06_01 to 2012_06_07
                        for(int i=1;i<8;i++)
                        {
                        File zipFile =new File("/scratch/morepvsforoneday/result/2012_01_0"+i+".zip");
                        InputStream in=GZIPUtil.unpackZip(zipFile,"pv0:2012_01_01.pb.gz");
                        //String filename=
                            GZIPUtil.unGZFile(in, "/scratch/morepvsforoneday/result/", "pv0:2012_01_0" + i + pbFileExtension);
                        //System.out.println(filename);
                        }
                        //GZIPUtil.unziptarFile(in, "pv54:2012_03_31_11.pb", "/scratch/", "new_pv54:2012_03_31_11.pb");
                        long time2=System.currentTimeMillis();
                        System.out.println("time consumed is :"+(time2-time1)+"ms");
                } catch (IOException e) {
                    logger.error("exception", e);
                } catch (Exception e) {
                    logger.error("exception", e);
                }
        }
        
        public static void getTimeOnePVForOneWeekFromOneYearZipPBFile()
        {
                long time1=System.currentTimeMillis();
                File zipFile =new File("/scratch/onepvperday/onePVALlPBOneYear.zip");
                try {
                        // search files for one week from 2012_06_01 to 2012_06_07
                        for(int i=1;i<8;i++)
                        {
                        InputStream in=GZIPUtil.unpackZip(zipFile,"pv:2012_06_0"+i+".pb.gz");
                        //String filename=
                            GZIPUtil.unGZFile(in, "/scratch/onepvperday/result/", "pv:2012_06_0" + i + pbFileExtension);
                        //System.out.println(filename);
                        }
                        //GZIPUtil.unziptarFile(in, "pv54:2012_03_31_11.pb", "/scratch/", "new_pv54:2012_03_31_11.pb");
                        long time2=System.currentTimeMillis();
                        System.out.println("time c  onsumed is :"+(time2-time1)+"ms");
                } catch (IOException e) {
                    logger.error("exception", e);
                } catch (Exception e) {
                    logger.error("exception", e);
                }
        }
        
        public static void getTimeonePVforOneHourPBFileFromOneMonthPBFile(){
                long time1=System.currentTimeMillis();
                File file =new File("/scratch/all.zip");
                try {
                        InputStream in=GZIPUtil.unpackZip(file,"pv54.tar.gz");
                        GZIPUtil.unziptarFile(in, "pv54:2012_03_31_11.pb", "/scratch/", "new_pv54:2012_03_31_11.pb");
                        long time2=System.currentTimeMillis();
                        System.out.println("time consumed is :"+(time2-time1)+"ms");
                } catch (IOException e) {
                    logger.error("exception", e);
                } catch (Exception e) {
                    logger.error("exception", e);
                }
        }

}
