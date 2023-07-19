package edu.stanford.slac.archiverappliance.PB.compression;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

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


    // path it the ROOT  DIRECTORY of all PB files


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
