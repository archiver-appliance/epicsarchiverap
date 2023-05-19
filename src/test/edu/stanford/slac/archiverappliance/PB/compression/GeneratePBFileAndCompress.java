package edu.stanford.slac.archiverappliance.PB.compression;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.ScalarDouble.Builder;
import edu.stanford.slac.archiverappliance.PB.data.PBScalarDouble;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

public class GeneratePBFileAndCompress {
	private static Logger logger = LogManager.getLogger(GeneratePBFileAndCompress.class.getName());
        
        //pb://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR
        /**
         * @param args
         */
        public static void main(String[] args) {
                try {
                	GeneratePBFileAndCompress.packAllPBFiles("/scratch/200000pvsforoneday/","2012_01_01.zip",true);
                } catch (Exception e) {
                	logger.error("exception", e);
                }
        }
        
        
        public static String generateAndCompressAndPack200000PVPBFileByDayExample(String channelName,String path, ConfigService configService)
        {
                try {
                        long time1=System.currentTimeMillis();
                        GeneratePBFileAndCompress.writeOnePvPBFileByDay(channelName, path,60*60*24*2, configService);
                        // copy pv:2012_01_01.pb 365 times.
                        /*Calendar cal=Calendar.getInstance();
                        cal.set(2012, 1, 0, 0, 0, 0);
                        SimpleDateFormat format=new SimpleDateFormat("yyyy_MM_dd");
                        */
                        //copy pb files
                        
                        GeneratePBFileAndCompress.compressAllPBFiles(path);
                        for(int i=0;i<200000;i++)
                        {
                                //cal.add(Calendar.DAY_OF_MONTH, 1);
                                String fileName="pv"+i+":2012_01_01.pb.gz";
                                GeneratePBFileAndCompress.copyFile("/scratch/200000pvsforoneday/", "pv:2012_01_01.pb.gz", "/scratch/200000pvsforoneday/", fileName);
                        }
                        String pathAndFileName=GeneratePBFileAndCompress.packAllPBFiles(path,"2012_01_01.zip",true);
                        System.out.println(pathAndFileName);
                        long time2=System.currentTimeMillis();
                        System.out.println("time consumed is "+(time2-time1)+"ms");
                        return pathAndFileName;
                } catch (Exception e) {
                    logger.error("exception", e);
                }
                return null;
        }
        
        public static String generateAndCompressAndPack5000PVsForOneWeekPBFileByDayExample(String channelName,String path, ConfigService configService)
        {
                try {
                        long time1=System.currentTimeMillis();
                        GeneratePBFileAndCompress.writeOnePvPBFileByDay(channelName, path,60*60*24*2, configService);
                        // copy pv:2012_01_01.pb 365 times.
                        /*Calendar cal=Calendar.getInstance();
                        cal.set(2012, 1, 0, 0, 0, 0);
                        SimpleDateFormat format=new SimpleDateFormat("yyyy_MM_dd");
                        */
                        //copy pb files
                        for(int i=0;i<5000;i++)
                        {
                                //cal.add(Calendar.DAY_OF_MONTH, 1);
                                String fileName="pv"+i+":2012_01_01.pb";
                                GeneratePBFileAndCompress.copyFile("/scratch/morepvsforoneday/", "pv:2012_01_01.pb", "/scratch/morepvsforoneday/", fileName);
                        }
                        
                        GeneratePBFileAndCompress.compressAllPBFiles(path);
                        String pathAndFileName=GeneratePBFileAndCompress.packAllPBFiles(path,"2012_01_01.zip",false);
                        System.out.println(pathAndFileName);
                        long time2=System.currentTimeMillis();
                        System.out.println("time consumed is "+(time2-time1)+"ms");
                        return pathAndFileName;
                } catch (Exception e) {
                    logger.error("exception", e);
                }
                return null;
        }
        
        // the zip file name generated is onePVALlPBOneYear.zip
        // return  the file path adn file name 
        public static String generateAndCompressAndPackOnePVForOneyearPBFileByDayExample(String channelName,String path, ConfigService configService)
        {
                try {
                        long time1=System.currentTimeMillis();
                        GeneratePBFileAndCompress.writeOnePvPBFileByDay(channelName, path,60*60*24*2, configService);
                        // copy pv:2012_01_01.pb 365 times.
                        Calendar cal=Calendar.getInstance();
                        cal.set(2012, 1, 0, 0, 0, 0);
                        SimpleDateFormat format=new SimpleDateFormat("yyyy_MM_dd");
                        
                        for(int i=0;i<365;i++)
                        {
                                cal.add(Calendar.DAY_OF_MONTH, 1);
                                String fileName="pv:"+format.format(cal.getTime())+".pb";
                                GeneratePBFileAndCompress.copyFile("/scratch/onepvperday/", "pv:2012_01_01.pb", "/scratch/onepvperday/", fileName);
                        }
                        
                        
                        GeneratePBFileAndCompress.compressAllPBFiles(path);
                        String pathAndFileName=GeneratePBFileAndCompress.packAllPBFiles(path,"onePVALlPBOneYear.zip",false);
                        long time2=System.currentTimeMillis();
                        System.out.println("time consumed is "+(time2-time1)+"ms");
                        return pathAndFileName;
                } catch (Exception e) {
                    logger.error("exception", e);
                }
                return null;
        }
        
        private static void copyFile(String sourcePath,String sourceFileName,String destPath,String destFileName) throws IOException
        {
                File destFile=new File(destPath+destFileName);
                if(destFile.isFile()&destFile.exists())
                {
                        destFile.delete();
                }
                FileInputStream in=new FileInputStream(sourcePath+sourceFileName);
                FileOutputStream out=new FileOutputStream(destFile);
                IOUtils.copy(in,out);
                out.flush();
                out.close();
                in.close();
                
        }

        public static void generate5000pvsPBFileByHourExample(ConfigService configService)
        {
                try {
                        long time1=System.currentTimeMillis();
                        GeneratePBFileAndCompress.generate5000pvsPBFileByHour("pv", "/scratch/test2/", configService);
                        long time2=System.currentTimeMillis();
                        System.out.println("time consumed is "+(time2-time1)+"ms");
                } catch (Exception e) {
                    logger.error("exception", e);
                }
        }
        
        
        
        public static void generate5000pvsPBFileByHour(String channelName,String path, ConfigService configService) throws Exception
        {
                for(int i=0;i<5000;i++)
                {
                        String tempPVName=channelName+i;
                        GeneratePBFileAndCompress.writePvsPBFileByHour(tempPVName, path, configService);
                        GeneratePBFileAndCompress.packAndCompressFile(tempPVName, path, configService);
                }
        }
        
        private static void writePvsPBFileByHour(String channelName,String path, ConfigService configService)
        {
        	try {
        		String des="pb://localhost?name=STS&rootFolder="+path+channelName+"&partitionGranularity=PARTITION_HOUR";
        		File tempFile=new File(path+channelName);
        		if(tempFile.exists())
        		{
        			if(!tempFile.isDirectory())
        			{
        				tempFile.mkdir();
        			}
        		}
        		else
        		{
        			tempFile.mkdir();
        		}

        		StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(des, configService);
        		Calendar cal=Calendar.getInstance();
        		cal.set(2012, 1, 0, 0, 0, 0);
        		long starttime=cal.getTimeInMillis();
        		//String channelName="test1";
        		int capacity=20000;
        		RemotableEventStreamDesc desc = new RemotableEventStreamDesc(
        				ArchDBRTypes.DBR_SCALAR_DOUBLE, channelName, (short)0);
        		for(int i=1;i<60*60*24;i++) {
        			try(ArrayListEventStream tempStream = new ArrayListEventStream(capacity, desc)) { 

        				Timestamp tl=new Timestamp(starttime+1000*i);
        				YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(tl);
        				//int year = yst.getYear();
        				Builder builder = EPICSEvent.ScalarDouble.newBuilder()
        						.setSecondsintoyear(yst.getSecondsintoyear())
        						.setNano(yst.getNanos())
        						.setVal(0);
        				builder.setSeverity(0);
        				builder.setStatus(0);
        				byte[] databytes = LineEscaper.escapeNewLines(builder.build().toByteArray());
        				PBScalarDouble tempPBScalarDouble=new PBScalarDouble(yst.getYear(), new ByteArray(databytes));

        				tempStream.add(tempPBScalarDouble);
        				if(i%capacity==0) {
        					try(BasicContext context = new BasicContext()) {
        						firstDest.appendData(context, channelName, tempStream);
        					}
        				}
        			}
        		}
        		//tempPBScalarDouble.
        	} catch (IOException e) {
        		logger.error("exception", e);
        	}
        }
        
        
        
        
        private static void writeOnePvPBFileByDay(String channelName,String path,long timeLength, ConfigService configService)
        {
        	///scratch/test/
        	try {
        		String des="pb://localhost?name=STS&rootFolder="+path+"&partitionGranularity=PARTITION_DAY";


        		StoragePlugin firstDest = StoragePluginURLParser.parseStoragePlugin(des, configService);
        		Calendar cal=Calendar.getInstance();
        		cal.set(2012, 0, 0, 0, 0, 0);
        		long starttime=cal.getTimeInMillis();
        		//String channelName="test1";
        		int capacity=20000;
        		RemotableEventStreamDesc desc = new RemotableEventStreamDesc(
        				ArchDBRTypes.DBR_SCALAR_DOUBLE, channelName, (short)0);
        		for(int i=1;i<timeLength;i++) {
        			try(ArrayListEventStream tempStream= new ArrayListEventStream(capacity, desc)) { 

        				Timestamp tl=new Timestamp(starttime+1000*i);
        				YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(tl);
        				//int year = yst.getYear();
        				Builder builder = EPICSEvent.ScalarDouble.newBuilder()
        						.setSecondsintoyear(yst.getSecondsintoyear())
        						.setNano(yst.getNanos())
        						.setVal(0);
        				builder.setSeverity(0);
        				builder.setStatus(0);
        				byte[] databytes = LineEscaper.escapeNewLines(builder.build().toByteArray());
        				PBScalarDouble tempPBScalarDouble=new PBScalarDouble(yst.getYear(), new ByteArray(databytes));

        				tempStream.add(tempPBScalarDouble);
        				if(i%capacity==0)
        				{
        					try(BasicContext context = new BasicContext()) {
        						firstDest.appendData(context, channelName, tempStream);
        					}
        				}
        			}
        		}
        		//tempPBScalarDouble.
        	} catch (IOException e) {
        		logger.error("exception", e);
        	}
        }
        
        // path it the ROOT  DIRECTORY of all PB files
        
        private static void packAndCompressFile(String channelName,String path, ConfigService configService) throws Exception
        {
                 File parentFile =new File(path+channelName);
                 if(parentFile.exists())
                 {
                         if(parentFile.isDirectory())
                         {
                                 File target=new File(path+channelName+".tar.gz");
                                 GZIPUtil.compress(GZIPUtil.packTar(parentFile.listFiles(), target));
                                 
                         }
                         else
                         {
                                 throw new Exception("File "+path+channelName+" is not a directory");
                         }
                 }
                 else
                 {
                         throw new Exception("File "+path+channelName+" does't exist");
                 }
        }
        
        
        
        private static void compressAllPBFiles(String parentPath) throws Exception
        {
                 File parentFile =new File(parentPath);
                 if(parentFile.exists())
                 {
                         if(parentFile.isDirectory())
                         {
                                 //File target=new File(path+channelName+".gz");
                                 //GZIPUtil.compress(GZIPUtil.pack(parentFile.listFiles(), target));
                                 
                                File [] files= parentFile.listFiles();
                                for(int i=0;i<files.length;i++)
                                {
                                        File tempFile=files[i];
                                        String fileName=tempFile.getName();
                                        //String targetFileName=fileName+".gz";
                                        //File target=new File(parentPath+targetFileName);
                                        if(fileName.endsWith(".pb"))
                                            GZIPUtil.compressGZ(tempFile,parentPath);
                                }
                                
                         }
                         else
                         {
                                 throw new Exception("File "+parentPath+" is not a directory");
                         }
                 }
                 else
                 {
                         throw new Exception("File "+parentPath+" does't exist");
                 }
        }
        
        private  static String packAllPBFiles(String parentPath,String destfileName,boolean useZip64) throws Exception
        {
                File parentFile =new File(parentPath);
                
                 if(parentFile.exists())
                 {
                         if(parentFile.isDirectory())
                         {
                                 //File target=new File(path+channelName+".gz");
                                 //GZIPUtil.compress(GZIPUtil.pack(parentFile.listFiles(), target));
                                 
                                File [] files= parentFile.listFiles();
                                ArrayList <File> filesList=new ArrayList <File>();
                                for(int i=0;i<files.length;i++)
                                {
                                        File tempFile=files[i];
                                        String fileName=tempFile.getName();
                                        if(fileName.endsWith("gz"))
                                        {
                                                filesList.add(tempFile);
                                        }
                                        //String targetFileName=fileName+".gz";
                                        //File target=new File(parentPath+targetFileName);
                                        //GZIPUtil.compress(tempFile);
                                }
                                //onePVALlPBOneYear.zip
                                String destFilePath=parentPath+destfileName;
                                File target=new File(destFilePath);
                                GZIPUtil.packZip(filesList, target,useZip64);
                                return destFilePath;
                                
                                
                                
                         }
                         else
                         {
                                 throw new Exception("File "+parentPath+" is not a directory");
                         }
                 }
                 else
                 {
                         throw new Exception("File "+parentPath+" does't exist");
                 }
        }

}
