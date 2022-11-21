/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *****
 */
package org.epics.archiverappliance.mgmt.archivepv;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.StorageMetrics;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.CPStaticData;
import org.epics.archiverappliance.mgmt.archivepv.CapacityPlanningData.ETLMetrics;
 
 
/**
* Primary capacity planning logic for the default capacity planning as outlined in the doc.
* @author luofeng
*
*/
public class CapacityPlanningBPL {
        private static Logger logger = Logger.getLogger(CapacityPlanningBPL.class.getName());
        private static boolean isDebug=false;
        /**
         * the percentage limitation for the ETL and Writer
         */
        private static float percentageLimitation=80;
        private static Logger configlogger = Logger.getLogger("config." + CapacityPlanningBPL.class.getName());
 
   /***
    * get the appliance for this pv.
    * this method implements the capacity planning
    * @param pvName the name of the pv.
    * @param configService the local configService 
    * @param pvTypeInfo the pvTypeInfo of this pv.
    * @return  the ApplianceInfo of the appliance which this pv will be added
    * @throws IOException  error occurs during the capacity planning.
    * in this case, this method will return the ApplianceInfo of the local  appliance
    */
        public static ApplianceInfo pickApplianceForPV(String pvName, ConfigService configService,PVTypeInfo pvTypeInfo) throws IOException {
             
                try{
                String [] dataStores=pvTypeInfo.getDataStores();
             
                HashMap<String,Integer>  dataStoresAddingpv=new HashMap<String,Integer>();
                for(String tempDataStores:dataStores)
                {
                        ETLSource tempETLSource=StoragePluginURLParser.parseETLSource(tempDataStores, configService);
                        if(tempETLSource==null) {
                                logger.debug("the ETLSource of "+tempDataStores+"is null");
                                if(isDebug)  logger.error("the ETLSource of "+tempDataStores+"is null");
                                continue;
                        }
                       
                        ETLDest  tempDest=StoragePluginURLParser.parseETLDest(tempDataStores, configService);
                        if (tempDest instanceof StorageMetrics)
                        {
                        	 int partitionSecond=tempETLSource.getPartitionGranularity().getApproxSecondsPerChunk();
                              String identifyTmep88= ((StorageMetrics)tempDest).getName();
                              dataStoresAddingpv.put(identifyTmep88, Integer.valueOf(partitionSecond));
                              if(isDebug) logger.error(identifyTmep88+"is added into dataStoresAddingpv");
                               
                        }       
                        
                }
 
                if(isDebug)   logger.error("dataStoresAddingpv.size()1="+dataStoresAddingpv.size());
 
          
                float pvstorageRate=pvTypeInfo.getComputedStorageRate();
 
                CPStaticData  cpStaticData=CapacityPlanningData.getMetricsForAppliances(configService);
                ConcurrentHashMap<ApplianceInfo, CapacityPlanningData> appliances=cpStaticData.cpApplianceMetrics;
 

                HashMap<ApplianceInfo, CapacityPlanningData>  nullETLMetricsAppliances=new HashMap<ApplianceInfo, CapacityPlanningData>();
                Iterator<Entry<ApplianceInfo, CapacityPlanningData>> it667 = appliances.entrySet().iterator();
        while(it667.hasNext())
             
              {
            
                Entry<ApplianceInfo, CapacityPlanningData> entry667 = (Entry<ApplianceInfo, CapacityPlanningData>)it667.next();
                CapacityPlanningData tempCapacityPlanningMetrics667=entry667.getValue();
 
 
                ApplianceInfo tempApplianceInfo667=entry667.getKey();
 
                //judge the appliance has all the destinatons of the pv adding
 
                ConcurrentHashMap<String, ETLMetrics> etlMetrics667=tempCapacityPlanningMetrics667.getEtlMetrics();
                 if (etlMetrics667.size()==0)
                 {
                 
                   logger.debug(tempApplianceInfo667.getIdentity()+" has no elements in the ETLMetrics");
                   if(isDebug)  logger.error(tempApplianceInfo667.getIdentity()+" has no elements in the ETLMetrics");
                  nullETLMetricsAppliances.put(tempApplianceInfo667, tempCapacityPlanningMetrics667);
                 }
 
              }
 
 
        if(nullETLMetricsAppliances.size()!=0)
        {
         //compare total pv envent rate of pv added and return the appliance
 
         Iterator<Entry<ApplianceInfo, CapacityPlanningData>> tempIt = appliances.entrySet().iterator();
            ArrayList<ApplianceAndTotalRate>  resultTempList=new ArrayList<ApplianceAndTotalRate>();
         while(tempIt.hasNext())
              
                  {
              
                    Entry<ApplianceInfo, CapacityPlanningData> entryTemp = (Entry<ApplianceInfo, CapacityPlanningData>)tempIt.next();
           
                    ApplianceInfo tempApplianceInfo99=entryTemp.getKey();
 
                   
                    
                    
                    CapacityPlanningData  tempCapacityPlanningMetricsPerApplianceForPV99=entryTemp.getValue();
                    ApplianceAggregateInfo tempApplianceAggregateInfo99=tempCapacityPlanningMetricsPerApplianceForPV99.getApplianceAggregateDifferenceFromLastFetch(configService);
                      
                         float   totalDataRate=(float)tempApplianceAggregateInfo99.getTotalStorageRate()+entryTemp.getValue().getCurrentTotalStorageRate();
               
 
 
                                 resultTempList.add(new ApplianceAndTotalRate(tempApplianceInfo99,totalDataRate));
 
                  }
 
         ApplianceAndTotalRate reulstApplianceInfo=null;
         for(int s=0;s<resultTempList.size();s++)
         {
                 ApplianceAndTotalRate temp=resultTempList.get(s);
                 if(reulstApplianceInfo==null )reulstApplianceInfo=temp;
                 if(temp.getTotalDataRate()<reulstApplianceInfo.getTotalDataRate())
                 {
                         reulstApplianceInfo=temp;
                 }
 
         }
 
         if(reulstApplianceInfo==null)
         {
                 throw (new Exception("reulstApplianceInfo is null"));
         }
         else
         {
        	      logger.debug("the ETLMetrics in one appliance of the cluster has no elements, so the capacity planning just uses the dataRate!");
        	      if(isDebug)  logger.error("the ETLMetrics in one appliance of the cluster has no elements, so the capacity planning just uses the dataRate!");
                  return  reulstApplianceInfo.getAppInfo();
         }
 
 
        }
 
 
 
 
 
                  Iterator<Entry<ApplianceInfo, CapacityPlanningData>> it = appliances.entrySet().iterator();
 
 
                  while(it.hasNext())
                        {
                         Entry<ApplianceInfo, CapacityPlanningData> entry = (Entry<ApplianceInfo, CapacityPlanningData>)it.next();
                         CapacityPlanningData tempCapacityPlanningMetrics=entry.getValue();
                         
                         tempCapacityPlanningMetrics.setAvailable(true);
 
 
                         //judge the appliance has all the destinatons of the pv adding
 
                         ConcurrentHashMap<String, ETLMetrics> etlMetrics=tempCapacityPlanningMetrics.getEtlMetrics();
                
                                 //////////////////////////////////////////////
                         
                                   
                                    ApplianceAggregateInfo tempApplianceAggregateInfo66=tempCapacityPlanningMetrics.getApplianceAggregateDifferenceFromLastFetch(configService);
                                   float totalDataRate=tempCapacityPlanningMetrics.getCurrentTotalStorageRate();
                                   
                                   float totalDataRateforpvadded=(float)tempApplianceAggregateInfo66.getTotalStorageRate();
                                   
                                 
                                    HashMap<String, Long> totalEstimageStoragePVAddedAndAddingBydifferentDestinationList=tempApplianceAggregateInfo66.getTotalStorageImpact();
                            
                                    
                                    Iterator<Entry<String, Integer>> itDataStoresAddingpv = dataStoresAddingpv.entrySet().iterator();
                   
                   
                                    while(itDataStoresAddingpv.hasNext())
                                          {
                                           Entry<String, Integer> entryTemp99 = (Entry<String, Integer>)itDataStoresAddingpv.next();
                   
                                           String identifypvAdding=entryTemp99.getKey();
                                           ETLMetrics  tempETLMetrics555=etlMetrics.get(identifypvAdding);
                                     	  if(tempETLMetrics555!=null)
                                     	  {
                                     		  tempETLMetrics555.estimateStoragePVadded=totalEstimageStoragePVAddedAndAddingBydifferentDestinationList.get(identifypvAdding);
                                     	  }   
                                          }
                                  
                          
                                  
                                  Iterator<Entry<String, Long>> itStoragePv66 = totalEstimageStoragePVAddedAndAddingBydifferentDestinationList.entrySet().iterator();
                                  while(itStoragePv66.hasNext())
                                      {
                                       Entry<String, Long> entryTemp66 = (Entry<String, Long>)itStoragePv66.next();
                                       Long tempToalEstimateStorage=entryTemp66.getValue();
                                       String tempIdentity66=entryTemp66.getKey();
                                       
                                  
                                      Integer tempSeconds= dataStoresAddingpv.get(tempIdentity66);
                                      if(tempSeconds!=null)
                                      {
                                    	  tempToalEstimateStorage= tempToalEstimateStorage+(long)(pvstorageRate*tempSeconds);
                                      }
                                      
                                      }
 
 
                                  ///////////////////////////////////until now, we have finished computing the estimate storage ////////////////////////////////////////////////////////
 
                                   ///////////////////////////////////////in the following, we will compare the estimate storage with the available storage of all applicance ////////
                                                                                  ///////////the estimate storage/////////////////////////////////////
                                   Iterator<Entry<String, ETLMetrics>> it7777 = etlMetrics.entrySet().iterator();
                                                      while(it7777.hasNext())
                                                     {
                                                           Entry<String, ETLMetrics> entry7777 = (Entry<String, ETLMetrics>)it7777.next();
                                                     ETLMetrics tempETLMetrics7777=entry7777.getValue();
                                                     String identity7777=tempETLMetrics7777.identity;
                                                       long alvailableStorage=tempETLMetrics7777.etlStorageAvailable;
                                                     
                                                       
                                                      Integer tempSeconds7777= dataStoresAddingpv.get(identity7777);
                                                       
                                                       if(tempSeconds7777!=null)
                                                       {
			                                                 long estimateStorageSize7777=totalEstimageStoragePVAddedAndAddingBydifferentDestinationList.get(identity7777);
			                                                 if(estimateStorageSize7777>alvailableStorage)
			                                                 {
			                                                         tempCapacityPlanningMetrics.setAvailable(false);
			                                                         configlogger.error("There is not enough storage to accommodate " + pvName+" for "+identity7777+
			                                                        		 ". Estimated storage for "+ pvName+" is "+estimateStorageSize7777+" while available storage is "+alvailableStorage);
			                                                         
			                                                         if(isDebug)   {
			                                                        	 
			                                                        	  logger.error(identity7777+":  testestimateStorageSize7777="+estimateStorageSize7777+",alvailableStorage="+alvailableStorage);
			                                                        	 logger.error("testestimateStorageSize7777>alvailableStorage");
			                                                         }
			                                                 }
                                                 
                                                       }else{
                                                    	   
                                                    	   if(isDebug)   logger.error("tempSeconds7777=null, and identity7777="+identity7777);
                                                       }
                                                       
                                                       
        
                                                           if(!tempCapacityPlanningMetrics.isAvailable()){
                                                        	   
                                                        	   if(isDebug)   logger.error("testtempCapacityPlanningMetrics.isAvailable()---1---");
                                                        	   break;
                                                           }
                                                     }// end while
                                                   if(!tempCapacityPlanningMetrics.isAvailable()) {
                                                	   if(isDebug)   logger.error("testtempCapacityPlanningMetrics.isAvailable()---2---");
                                                	   continue;
                                                   }
                                 
                                             float currentUsedWriterPercentage=tempCapacityPlanningMetrics.getEngineWriteThreadUsage(PVTypeInfo.getSecondsToBuffer(configService));
                                             if(currentUsedWriterPercentage>CapacityPlanningBPL.percentageLimitation) {
                                            	 tempCapacityPlanningMetrics.setAvailable(false);
                                            	 if(isDebug)  logger.error("test exceed the limitation");
                                            	 configlogger.error("There is not enough time left for writer to write " + pvName+
                                            			 " into short term storage. Estimated writing percentage for "+pvName+
                                            			 " is "+currentUsedWriterPercentage+" while the percentage limitation is "+CapacityPlanningBPL.percentageLimitation);
                                             }
                                                  float percentageForWriter=currentUsedWriterPercentage*(totalDataRateforpvadded+totalDataRate)/(totalDataRate);
                                            tempCapacityPlanningMetrics.setPercentageTimeForWriter(percentageForWriter);
                                            if(percentageForWriter>CapacityPlanningBPL.percentageLimitation){
                                            	 tempCapacityPlanningMetrics.setAvailable(false);
                                            	
                                            	 configlogger.error("There is not enough time left for writer to write " + pvName+
                                            			 " into short term storage. Estimated writing percentage for "+pvName+
                                            			 " is "+percentageForWriter+" while the percentage limitation is "+CapacityPlanningBPL.percentageLimitation);
                                            	 if(isDebug) logger.error("testpercentageForWriter>CapacityPlanningBPL.percentageLimitation");
                                            }
                                                
                                            //normalize ETL time   
                                            //the time consumed by ETL=((the estimate storage size+usedstorage)/usedstorage)*TimeofETl.
                                           // the estimate storage size=sum(pvaddedDataRate*partitionTime);
                                            Iterator<Entry<String, ETLMetrics>> it8888 = etlMetrics.entrySet().iterator();
                                                      while(it8888.hasNext())
                                                     {
                                                           Entry<String, ETLMetrics> entry8888 = (Entry<String, ETLMetrics>)it8888.next();
                                                     ETLMetrics tempETLMetrics8888=entry8888.getValue();
                                                     long storageUsed=tempETLMetrics8888.totalSpace-tempETLMetrics8888.etlStorageAvailable;
                                                     double temptempETLTimeTaken=tempETLMetrics8888.etlTimeTaken;
                                                     //if(temptempETLTimeTaken>60)
                                                     if(temptempETLTimeTaken>CapacityPlanningBPL.percentageLimitation)
                                                     {
                                                    	 tempCapacityPlanningMetrics.setAvailable(false);
                                                    	 configlogger.error("There is not enough time left for ETL to write " + pvName+" into "+tempETLMetrics8888.identity+
                                                    			 ". Estimated percentage time is "+temptempETLTimeTaken+" while the percentage limitation is "+CapacityPlanningBPL.percentageLimitation);
                                                    	 if(isDebug) logger.error("testtemptempETLTimeTaken>CapacityPlanningBPL.percentageLimitation");
                                                     }
                                                        
                                                     double tempEstimateETLtimePercentageAfterPVadded=temptempETLTimeTaken*(double)(storageUsed+tempETLMetrics8888.estimateStoragePVadded)/(double)storageUsed;
                                                     tempETLMetrics8888.estimateETLtimePercentageAfterPVadded=tempEstimateETLtimePercentageAfterPVadded;
 
	                                                     if(tempEstimateETLtimePercentageAfterPVadded>CapacityPlanningBPL.percentageLimitation){
	                                                    	 if(isDebug) logger.error("testtempEstimateETLtimePercentageAfterPVadded>CapacityPlanningBPL.percentageLimitation");
	                                                         tempCapacityPlanningMetrics.setAvailable(false);
	                                                         configlogger.error("There is not enough time left for ETL to write " + pvName+" into "+tempETLMetrics8888.identity+
	                                                        		 ". Estimated percentage time is "+tempEstimateETLtimePercentageAfterPVadded+" while the percentage limitation is "+CapacityPlanningBPL.percentageLimitation);
	                                                     }
                                                     }
                        if(isDebug)logger.error(entry.getKey().getIdentity()+" is called");
 
                        }//end while
 
        ///////////////////////////////////////////////////////////////until now ,finish the normalization//////next step is to find the maximum of the average///////////////////////////////////////////////////////////////////////////////////
                // compute the average of the time percent of the writer
                  Iterator<Entry<ApplianceInfo, CapacityPlanningData>> it33 = appliances.entrySet().iterator();
                  //totalDataList
                  float averagePercentageWriter=0;
                 
                  int availableAppliancesNum=0;
       
                  HashMap<String,Double> averagePercentageETL=new HashMap<String,Double>();
                  
                  if(isDebug) logger.error("dataStoresAddingpv.size()2="+dataStoresAddingpv.size());
                  while(it33.hasNext())
                        {
                	  
                	  // list through all appliances
                         Entry<ApplianceInfo, CapacityPlanningData> entry33 = (Entry<ApplianceInfo, CapacityPlanningData>)it33.next();
                         CapacityPlanningData tempCapacityPlanningMetrics33=entry33.getValue();
                         if (!tempCapacityPlanningMetrics33.isAvailable()){
                        	 if(isDebug)	logger.error( entry33.getKey().getIdentity()+" is not available");
                        	 continue;
                         }
                         availableAppliancesNum++;
                         //compute writer time
                         averagePercentageWriter+=tempCapacityPlanningMetrics33.getPercentageTimeForWriter();
                         ConcurrentHashMap<String, ETLMetrics> ETLMetrics=tempCapacityPlanningMetrics33.getEtlMetrics();
                         
                         if(isDebug) logger.error("ETLMetrics.size()="+ETLMetrics.size());
                         //compute ETL Time
                         
                         Iterator<Entry<String, Integer>> itDataStoresAddingpv66 = dataStoresAddingpv.entrySet().iterator();
                    
        
                         // through all etls in one appliance
                         while(itDataStoresAddingpv66.hasNext())
                               {
                                Entry<String, Integer> entryTemp9966 = (Entry<String, Integer>)itDataStoresAddingpv66.next();
        
                                String identifypvAdding66=entryTemp9966.getKey();
                                ETLMetrics tempETLMetrics33=ETLMetrics.get(identifypvAdding66);
                                if(tempETLMetrics33!=null)
                                {
                                        Double TempValue=averagePercentageETL.get(identifypvAdding66);
                                        if(TempValue==null)
                                        {
                                        	 averagePercentageETL.put(identifypvAdding66, Double.valueOf(tempETLMetrics33.estimateETLtimePercentageAfterPVadded));
                                        	 if(isDebug) logger.error(identifypvAdding66+" is null ");
                                        }else
                                        {
                                        	 averagePercentageETL.put(identifypvAdding66, Double.valueOf(TempValue+tempETLMetrics33.estimateETLtimePercentageAfterPVadded));
                                        	 if(isDebug) logger.error(identifypvAdding66+" is added ");
                                        }

                                }
                               }
                      
                        }
 
                  ArrayList<NormalizationFactor> allNormalizationFactor=new ArrayList<NormalizationFactor>();
                  // compute average.
                 if(isDebug) logger.error("availableAppliancesNum="+availableAppliancesNum);
                 if(availableAppliancesNum==0){
                	 configlogger.error("there is no appliance available and use the local appliance as the default");
                     return configService.getMyApplianceInfo();
                 }
                  averagePercentageWriter=averagePercentageWriter/availableAppliancesNum;
                  allNormalizationFactor.add(new NormalizationFactor("writer",averagePercentageWriter) );
 
                  Iterator<Entry<String, Double>> itaveragePercentageETL = averagePercentageETL.entrySet().iterator();
 
 
                  while(itaveragePercentageETL.hasNext())
                        {
                         Entry<String, Double> entryTemp99666 = (Entry<String, Double>)itaveragePercentageETL.next();
 
                         String identifypvAdding6666=entryTemp99666.getKey();
                         Double tempPercentageETL=entryTemp99666.getValue();
                         averagePercentageETL.put(identifypvAdding6666, Double.valueOf(tempPercentageETL/availableAppliancesNum));
                         allNormalizationFactor.add(new NormalizationFactor(identifypvAdding6666,tempPercentageETL.floatValue()) );
                         
                        }
 
                  //get the max of the average
                  //this is the max result
                  NormalizationFactor  resultFactor=new NormalizationFactor("tempFactor",0);
                  for(int s11=0;s11<allNormalizationFactor.size();s11++)
                  {
                          NormalizationFactor tempNormalizationFactor11=allNormalizationFactor.get(s11);
                          if (tempNormalizationFactor11.getPercentage()>=resultFactor.getPercentage())
                          {
                                  resultFactor=tempNormalizationFactor11;
                                  
                          }
                  }
                  if(resultFactor.getIdentify().equals("tempFactor"))
                  {
                	  
                	  if(isDebug)  logger.error("allNormalizationFactor.size()="+allNormalizationFactor.size()); 
                	  configlogger.error("there are some error in computing the max (percentage)");
                	  configlogger.error("there is one error during capaicity planning computing and use the local appliance as the default");
                return configService.getMyApplianceInfo();
                  }
                  logger.debug("the max (percentage) is :"+resultFactor.getIdentify()+",value:"+resultFactor.getPercentage());
 
                ApplianceInfo minResultApplianceInfo=null;
                String minIdentify=resultFactor.getIdentify();
                if(minIdentify.equals("writer"))
                {
 
	                 Iterator<Entry<ApplianceInfo, CapacityPlanningData>> it88 = appliances.entrySet().iterator();
	 
	 
	                  while(it88.hasNext())
	                        {
		                         Entry<ApplianceInfo, CapacityPlanningData> entry88 = (Entry<ApplianceInfo, CapacityPlanningData>)it88.next();
		                         CapacityPlanningData tempCapacityPlanningMetric88=entry88.getValue();
		 
		 
		                         ApplianceInfo tempApplianceInfo88=entry88.getKey();
		                         if (!tempCapacityPlanningMetric88.isAvailable()) continue;
		                         if(minResultApplianceInfo==null)  minResultApplianceInfo=tempApplianceInfo88;
		 
		                         CapacityPlanningData minCapacityPlanningMetric=appliances.get(minResultApplianceInfo);
		                         float minPercentageTimeWriter=minCapacityPlanningMetric.getPercentageTimeForWriter();
		                         float tempPercentageTimeWriter88=tempCapacityPlanningMetric88.getPercentageTimeForWriter();
		                         if(tempPercentageTimeWriter88<minPercentageTimeWriter) minResultApplianceInfo=tempApplianceInfo88;
	 
	                         }
                 }
                else
                {
                          Iterator<Entry<ApplianceInfo, CapacityPlanningData>> it88 = appliances.entrySet().iterator();
 
 
                          while(it88.hasNext())
                                {
	                                 Entry<ApplianceInfo, CapacityPlanningData> entry88 = (Entry<ApplianceInfo, CapacityPlanningData>)it88.next();
	                                 CapacityPlanningData tempCapacityPlanningMetric88=entry88.getValue();
	 
	 
	                                 ApplianceInfo tempApplianceInfo88=entry88.getKey();
	                                 if (!tempCapacityPlanningMetric88.isAvailable()) continue;
	                                 if(minResultApplianceInfo==null)  minResultApplianceInfo=tempApplianceInfo88;
	                                 CapacityPlanningData minCapacityPlanningMetric=appliances.get(minResultApplianceInfo);
	 
	                                 ConcurrentHashMap<String, ETLMetrics> minETLMetrics=minCapacityPlanningMetric.getEtlMetrics();
	 
	                                 ETLMetrics minETLMetric= minETLMetrics.get(minIdentify);
	                                 double minEstimateETLtimePercentageAfterPVadded=minETLMetric.estimateETLtimePercentageAfterPVadded;
	 
	 
	 
	                                 ConcurrentHashMap<String, ETLMetrics> tempETLMetrics88=tempCapacityPlanningMetric88.getEtlMetrics();
	 
	                                 ETLMetrics tempETLMetric88= tempETLMetrics88.get(minIdentify);
	                                 double estimateETLtimePercentageAfterPVadded88=tempETLMetric88.estimateETLtimePercentageAfterPVadded;
	                                 if(estimateETLtimePercentageAfterPVadded88<minEstimateETLtimePercentageAfterPVadded)
		                                 {
		                                         minResultApplianceInfo=tempApplianceInfo88;
		                                 }
	 
 
                                }
                }
                        
 
              if(minResultApplianceInfo==null){
            	  logger.error("there is one error during capaicity planning computing and use the local appliance as the default");
            	    return configService.getMyApplianceInfo();
              }
              return minResultApplianceInfo;
              
                }catch(Exception e)
                {
                        logger.error("Exception during capacity planning, returning this appliance", e);
                        return configService.getMyApplianceInfo();
                }
               
        }
}
