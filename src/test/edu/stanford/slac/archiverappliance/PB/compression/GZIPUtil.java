package edu.stanford.slac.archiverappliance.PB.compression;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.log4j.Logger;



public class GZIPUtil {
	private static Logger logger = Logger.getLogger(GZIPUtil.class.getName());
   public static InputStream  unpackZip(File source, String fileName) throws IOException
   {
           ZipFile zipFile=new ZipFile(source);
           for(Enumeration<ZipArchiveEntry> files=zipFile.getEntries();files.hasMoreElements();)
           {
                   ZipArchiveEntry zae=files.nextElement();
                   String tempFileName=zae.getName();
                  // System.out.println(tempFileName);
                   if(tempFileName.equals(fileName))
                   {
                           //ZipArchiveInputStream zipIn=new ZipArchiveInputStream(zipFile.getInputStream(zae));
                           //zipFile.getInputStream(zae);
                           ;
                           //TempGZIn.
                           return  zipFile.getInputStream(zae);
                           
                           
                   }
           }
           
           System.out.println("the result is null");
           return null;
   }
        
   
   public static String unGZFile(InputStream  inputStrem,String targetPath,String newFIleName) throws Exception 
   {
           GzipCompressorInputStream gzIn = new GzipCompressorInputStream(inputStrem);
           FileOutputStream out=new FileOutputStream(targetPath+newFIleName);
             
                IOUtils.copy(gzIn,out);
                out.flush();
                out.close();
                gzIn.close();
                inputStrem.close();
           //gzIn.re
                return  targetPath+newFIleName;
   }
   
   
   public static void unziptarFile(InputStream  inputStrem,String fileName,String targetPath,String newFIleName) throws Exception 
   {
          // GzipCompressorInputStream gzIn = new GzipCompressorInputStream(inputStrem); 
           FileOutputStream  out=null;
           try{
           //ArchiveInputStream in = new ArchiveStreamFactory().createArchiveInputStream("tar", gzIn);
                   
                   
                   ArchiveInputStream in = new ArchiveStreamFactory().createArchiveInputStream("tar", inputStrem);

          // BufferedInputStream bufferedInputStream = new BufferedInputStream(in);

       TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();
       //BufferedOutputStream bufout=new BufferedOutputStream();
         out=new FileOutputStream (targetPath+newFIleName);
       while (entry != null) {

          String name = entry.getName();
         // System.out.println(name);
          if(name.endsWith(fileName))
          {
           System.out.println("file is found");
           byte[] bs = new byte[2048];  
              int len = -1;  
              while ((len = in.read(bs)) != -1) {  
                  out.write(bs, 0, len);  
              }  
              out.flush();  
            
           break;  
          }
          entry = (TarArchiveEntry) in.getNextEntry();
       }
           }catch(Exception e)
           {
                   logger.error("Exception", e);
                   throw e;
           }
       finally
       {
            inputStrem.close();
            out.flush();
            out.close(); 
       }
           
   }
 
        
        
        public static File packZip(ArrayList<File> sources, File target,boolean useZip64) {
                File [] files=new File [sources.size()];
                for(int i=0;i<sources.size();i++)
                {
                        files[i]=sources.get(i);
                }
                return packZip(files,target,useZip64);
        }
        
        
        
        public static File packZip(File[] sources, File target,boolean useZip64) {

               FileOutputStream out = null;

               try {

                   out = new FileOutputStream(target);

               } catch (FileNotFoundException e1) {

                   logger.error("Exception", e1);

               }

               ZipArchiveOutputStream os = new ZipArchiveOutputStream(out);
               if(useZip64)  os.setUseZip64(Zip64Mode.Always);

               for (int i = 0; i < sources.length; i++) {

                   File file = sources[i];

                   try {
                  
                     ZipArchiveEntry zipEntry= new ZipArchiveEntry(file.getName());
                     zipEntry.setSize(file.length());
                      os.putArchiveEntry(zipEntry);
                      FileInputStream tempin= new FileInputStream(file);
                      IOUtils.copy(tempin, os);
                  
                      os.closeArchiveEntry();
                      tempin.close();

         

                   } catch (FileNotFoundException e) {

                      logger.error("Exception", e);

                   } catch (IOException e) {

                      logger.error("Exception", e);

                   }

               }

               if (os != null) {

                   try {

                      os.flush();

                      os.close();

                   } catch (IOException e) {

                      logger.error("Exception", e);

                   }

               }

         

               return target;

            }

    /**

     *

     * @Title: pack

     * @Description: tar

     * @param sources

     * @param target 

     * @return File   

     * @throws

     */

    public static File packTar(File[] sources, File target) {

       FileOutputStream out = null;

       try {

           out = new FileOutputStream(target);

       } catch (FileNotFoundException e1) {

           logger.error("Exception", e1);

       }

       TarArchiveOutputStream os = new TarArchiveOutputStream(out);

       for (int i = 0; i < sources.length; i++) {

           File file = sources[i];

           try {

              os.putArchiveEntry(new TarArchiveEntry(file));
              FileInputStream tempIn=new FileInputStream(file);
              IOUtils.copy(tempIn, os);

              os.closeArchiveEntry();
              tempIn.close();
 

           } catch (FileNotFoundException e) {

              logger.error("Exception", e);

           } catch (IOException e) {

              logger.error("Exception", e);

           }

       }

       if (os != null) {

           try {

              os.flush();

              os.close();

           } catch (IOException e) {

              logger.error("Exception", e);

           }

       }

 

       return target;

    }

 

    /**

     *

     * @Title: compress

     * @Description:

     * @param  source

     * @return File    

     * @throws

     */

    public static File compress(File source) {

       File target = new File(source.getName());

       FileInputStream in = null;

       GZIPOutputStream out = null;

       try {

           in = new FileInputStream(source);

           out = new GZIPOutputStream(new FileOutputStream(target));

           byte[] array = new byte[1024];

           int number = -1;

           while ((number = in.read(array, 0, array.length)) != -1) {

              out.write(array, 0, number);

           }

       } catch (FileNotFoundException e) {

           logger.error("Exception", e);

           return null;

       } catch (IOException e) {

           logger.error("Exception", e);

           return null;

       } finally {

           if (in != null) {

              try {

                  in.close();

              } catch (IOException e) {

                  logger.error("Exception", e);

                  return null;

              }

           }

 

           if (out != null) {

              try {

                  out.close();

              } catch (IOException e) {

                  logger.error("Exception", e);

                  return null;

              }

           }

       }

       return target;

    }

   
    
    public static File compressGZ(File source,String destPath) {

        File target = new File(destPath+source.getName()+".gz");

        FileInputStream in = null;

        GZIPOutputStream out = null;

        try {

            in = new FileInputStream(source);

            out = new GZIPOutputStream(new FileOutputStream(target));

            byte[] array = new byte[1024];

            int number = -1;

            while ((number = in.read(array, 0, array.length)) != -1) {

               out.write(array, 0, number);

            }

        } catch (FileNotFoundException e) {

            logger.error("Exception", e);

            return null;

        } catch (IOException e) {

            logger.error("Exception", e);

            return null;

        } finally {

            if (in != null) {

               try {

                   in.close();

               } catch (IOException e) {

                   logger.error("Exception", e);

                   return null;

               }

            }

  

            if (out != null) {

               try {

                   out.close();

               } catch (IOException e) {

                   logger.error("Exception", e);

                   return null;

               }

            }

        }

        return target;

     }


    public static void main(String[] args) {

      /* File[] sources = new File[] { new File("E:/a.txt"),

              new File("E:/b.txt") };

       File target = new File("E:/test.tar.gz");

       compress(pack(sources, target));
       
       */
         
         File source = new File("/scratch/new_pv54:2012_03_31_11.pb");
         compressGZ(source,"/scratch/");
         
       

    }
}