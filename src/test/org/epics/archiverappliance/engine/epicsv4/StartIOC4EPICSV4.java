package org.epics.archiverappliance.engine.epicsv4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.epics.ioc.database.PVDatabase;
import org.epics.ioc.database.PVDatabaseFactory;
import org.epics.ioc.database.PVRecord;
import org.epics.ioc.install.Install;
import org.epics.ioc.install.InstallFactory;
import org.epics.pvData.pv.MessageType;  
import org.epics.pvData.pv.PVStructure;
import org.epics.pvData.pv.Requester;


public class StartIOC4EPICSV4 {


	    private static final PVDatabase masterPVDatabase = PVDatabaseFactory.getMaster();
	    private static final Install install = InstallFactory.get();
	    /**
	     * read and dump a database instance file.
	     * @param  args is a sequence of flags and filenames.
	     */
	    public static void main(String[] args) {
	      
	        Requester iocRequester = new Listener();
	       
	           
	                parseStructures("./src/test/org/epics/archiverappliance/engine/epicsv4/example/structrue/structures.xml",iocRequester);
	          
	                parseRecords("./src/test/org/epics/archiverappliance/engine/epicsv4/example/exampleDB.xml",iocRequester);
	            
	                startServer("./src/test/org/epics/archiverappliance/engine/epicsv4/pvAccessServer.txt");
	          //      SwtshellFactory.swtshell();
	           
	              
	        
	        
	        //end while
	    }
	    
	    static void printError(String message) {
	        System.err.println(message);
	    }
	    
	    @SuppressWarnings("unchecked")
		static void startServer(String fileName) {
	        System.out.println("starting servers fileName " + fileName);
	        try {
	            BufferedReader in = new BufferedReader(new FileReader(fileName));
	            String factoryName = null;
	            while((factoryName = in.readLine()) !=null) {
	                System.out.println("starting server factoryName " + factoryName);
	                @SuppressWarnings("rawtypes")
					Class startClass;
	                Method method = null;
	                try {
	                    startClass = Class.forName(factoryName);
	                }catch (ClassNotFoundException e) {
	                    printError("server factory "
	                            + e.getLocalizedMessage()
	                            + " class not found");
	                    continue;
	                }
	                try {
	                    method = startClass.getDeclaredMethod("start", (Class[])null);
	                } catch (NoSuchMethodException e) {
	                    printError("server factory "
	                            + e.getLocalizedMessage()
	                            + " method start not found");
	                    continue;
	                }
	                if(!Modifier.isStatic(method.getModifiers())) {
	                    printError("server factory "
	                            + factoryName
	                            + " start is not a static method ");
	                    continue;
	                }
	                try {
	                    method.invoke(null, new Object[0]);
	                } catch(IllegalAccessException e) {
	                    printError("server start IllegalAccessException "
	                            + e.getLocalizedMessage());
	                    continue;
	                } catch(IllegalArgumentException e) {
	                    printError("server start IllegalArgumentException "
	                            + e.getLocalizedMessage());
	                    continue;
	                } catch(InvocationTargetException e) {
	                    printError("server start InvocationTargetException "
	                            + e.getLocalizedMessage());
	                    continue;
	                }
	            }
	            in.close();
	        } catch (IOException e) {
	            System.err.println("startServer error " + e.getMessage());
	            return;
	        }
	    }
	        
	    static void dumpStructures() {
	        PVStructure[] pvStructures = masterPVDatabase.getStructures();
	        if(pvStructures.length>0) System.out.printf("\n\nstructures");
	        for(PVStructure pvStructure : pvStructures) {
	            System.out.print(pvStructure.toString());
	        }
	    }
	    
	    static void dumpRecords() {
	        PVRecord[] pvRecords = masterPVDatabase.getRecords();
	        if(pvRecords.length>0) System.out.printf("\n\nrecords");
	        for(PVRecord pvRecord : pvRecords) {
	            System.out.print(pvRecord.toString());
	        }
	    }

	    static void parseStructures(String fileName,Requester iocRequester) {
	    	long startTime = 0;
	    	long endTime = 0;
	    	startTime = System.nanoTime();
	    	try {
	    		install.installStructures(fileName,iocRequester);
	    	}  catch (IllegalStateException e) {
	    		System.out.println("IllegalStateException: " + e);
	    	}
	    	endTime = System.nanoTime();
	    	double diff = (double)(endTime - startTime)/1e9;
	    	System.out.printf("\ninstalled structures %s time %f seconds\n",fileName,diff);
	    }

	    static void parseRecords(String fileName,Requester iocRequester) {
	    	long startTime = 0;
	    	long endTime = 0;
	    	startTime = System.nanoTime();
	    	try {
	    		install.installRecords(fileName,iocRequester);
	    	}  catch (IllegalStateException e) {
	    		System.out.println("IllegalStateException: " + e);
	    	}
	    	endTime = System.nanoTime();
	    	double diff = (double)(endTime - startTime)/1e9;
	    	System.out.printf("\ninstalled records %s time %f seconds\n",fileName,diff);
	    }
	     
	    private static class Listener implements Requester {
	        /* (non-Javadoc)
	         * @see org.epics.ioc.util.Requester#getRequesterName()
	         */
	        public String getRequesterName() {
	            return "javaIOC";
	        }
	        /* (non-Javadoc)
	         * @see org.epics.ioc.util.Requester#message(java.lang.String, org.epics.ioc.util.MessageType)
	         */
	        public void message(String message, MessageType messageType) {
	            System.out.println(message);
	            
	        }
	    }
}
