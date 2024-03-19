package org.epics.archiverappliance;

import gov.aps.jca.CAException;
import gov.aps.jca.Channel;
import gov.aps.jca.Context;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.TimeoutException;
import gov.aps.jca.configuration.Configuration;
import gov.aps.jca.configuration.ConfigurationException;
import gov.aps.jca.configuration.DefaultConfigurationBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.exception.ConfigException;
import org.epics.archiverappliance.engine.epics.JCAConfigGen;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class PVCaPut {
	private static Logger logger = LogManager.getLogger(PVCaPut.class.getName());

          /**
   * JCA context.
   */
  private Context context = null;
  private JCALibrary jca =null;
         /**
     * Initialize JCA context.
     * @throws CAException      throws on any failure.
         * @throws ConfigurationException 
         * @throws IOException 
         * @throws SAXException 
     */
    private void initialize() throws CAException, SAXException, IOException, ConfigurationException, ConfigException {

        ConfigServiceForTests configService = new ConfigServiceForTests(-1);
                // Get the JCALibrary instance.
         if(jca==null)
                 jca = JCALibrary.getInstance();
           
                        ByteArrayInputStream bis = JCAConfigGen.generateJCAConfig(configService);
                DefaultConfigurationBuilder configBuilder = new DefaultConfigurationBuilder();
                Configuration configuration;
                
                        configuration = configBuilder.build(bis);

                        // Create a context with default configuration values.
                        if(context==null)
                                context = jca.createContext(configuration);
    }
    
    
    /**
     * Destroy JCA context.
     */
    private void destroy() {
        
        try {

            // Destroy the context, check if never initialized.
            if (context != null)
                context.destroy();
            
        } catch (Throwable t) {
            logger.error("Exception destroying context", t);
        }
    }
   
    
    public void caPut(String pvName,double value) throws CAException, IllegalStateException, TimeoutException, SAXException, IOException, ConfigurationException, ConfigException {
         initialize();
          Channel channel = context.createChannel(pvName);
          context.pendIO(3.0);
          channel.put(value);
          context.pendIO(3.0);
          channel.destroy();      
          destroy();
    }

    public void caPut(String pvName,String value) throws CAException, IllegalStateException, TimeoutException, SAXException, IOException, ConfigurationException, ConfigException {
        initialize();
         Channel channel = context.createChannel(pvName);
         context.pendIO(3.0);
         channel.put(value);
         context.pendIO(3.0);
         channel.destroy();      
         destroy();
   }

    /**
     * caput values into PV
     * @param pvName
     * @param values
     * @param timebetweenupdates - Sleep for this many mills between updates.
     * @throws Exception
     */
    public void caPutValues(String pvName,double[] values, int timebetweenupdates) throws Exception  {
    	initialize();
    	Channel channel = context.createChannel(pvName);
    	context.pendIO(3.0);
    	for(double value : values) { 
        	channel.put(value);
        	context.pendIO(3.0);
        	Thread.sleep(timebetweenupdates);
    	}
    	channel.destroy();      
    	destroy();
   }

        /**
         * @param args  
         */
        public static void main(String[] args) throws Exception {
                try {
                        try {
                                new PVCaPut().caPut("test:enable0", 1);
                        } catch (SAXException | IOException | ConfigurationException e) {
                    		logger.error(e);
                        }
                } catch (IllegalStateException | CAException | TimeoutException e) {
            		logger.error(e);
                }

        }

}