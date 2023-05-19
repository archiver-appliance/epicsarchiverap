/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.model;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;




/** Logger that only allows a certain message rate.
 *  @author Kay Kasemir
 */
public class ThrottledLogger
{
	
    static private final Logger logger = LogManager.getLogger(ThrottledLogger.class);
    /** Log level */
    final private LogLevel level;

    /** Throttle for the message rate */
    final private Throttle throttle;
  
    /** <code>true</code> when in the 'be quiet' state */
    private boolean throttled = false;



    /** Initialize
     *  @param level Log level to use
     *  @param seconds_between_messages Seconds between allowed messages
     */
    public ThrottledLogger(final LogLevel level,
            final double seconds_between_messages)
    {
        this.level = level;
        throttle = new Throttle(seconds_between_messages);
    }

    /** Add throttled info message to the plugin log. 
     * @param message  &emsp;
     * @return boolean True or False
     */
    @SuppressWarnings("nls")
    public boolean log( final String message)
    {
        if (throttle.isPermitted())
        {   // OK, show
        	//logger.log(level, message);
        	
        	if(level==LogLevel.error)
        	{
        		logger.error(message);
        	}
        	else if(level==LogLevel.warning)
        	{
        		logger.warn(message);
        	} else if(level==LogLevel.info)
        	{
        		logger.info(message);	
        	}
            throttled = false;
            return true;
        }
        // Show nothing
        if (throttled)
            return false;
        // Last message to be shown for a while
        
        
       String messageTemp= message
                + "\n... More messsages suppressed for "
                + PeriodFormat.formatSeconds(throttle.getPeriod())
                + " ....";
       
       if(level==LogLevel.error)
   	{
   		logger.error(messageTemp);
   	}
   	else if(level==LogLevel.warning)
   	{
   		logger.warn(messageTemp);
   	} else if(level==LogLevel.info)
   	{
   		logger.info(messageTemp);	
   	}
       
        throttled = true;
        return true;
    }
}
