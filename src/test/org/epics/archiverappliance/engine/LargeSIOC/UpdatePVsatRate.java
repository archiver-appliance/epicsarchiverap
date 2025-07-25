/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.LargeSIOC;

import gov.aps.jca.CAException;
import gov.aps.jca.Channel;
import gov.aps.jca.Context;
import gov.aps.jca.JCALibrary;
import gov.aps.jca.configuration.Configuration;
import gov.aps.jca.configuration.DefaultConfigurationBuilder;
import gov.aps.jca.event.ConnectionEvent;
import gov.aps.jca.event.ConnectionListener;
import gov.aps.jca.event.PutEvent;
import gov.aps.jca.event.PutListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.epics.JCAConfigGen;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Update a specified number of sine_* PV's generated by GenerateLargeDB once a second using ca_put at a certain rate
 * The values are set to generate sine waves with a frequency of 1/3600Hz and a phase related to the pv number
 * @author mshankar
 */
public class UpdatePVsatRate {
    private static final Logger logger = LogManager.getLogger(UpdatePVsatRate.class);
    private static Context JCAContext = null;
    private static LinkedBlockingQueue<Runnable> contextTasks = new LinkedBlockingQueue<Runnable>();
    private static ScheduledThreadPoolExecutor eventGenerator = new ScheduledThreadPoolExecutor(1);
    private ConcurrentHashMap<String, UpdateValue> connectedChannels = null;
    private ConfigServiceForTests configService = new ConfigServiceForTests(-1);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "Usage: java org.epics.archiverappliance.engine.LargeSIOC.UpdatePVsatRate <TotalPVs> <Rate>");
            return;
        }

        int totalpvcount = Integer.parseInt(args[0]);
        int rate = Integer.parseInt(args[1]);
        UpdatePVsatRate upv = new UpdatePVsatRate(totalpvcount, rate);
        upv.processContextTasks();
    }

    public UpdatePVsatRate(final int totalpvcount, final int rate) throws Exception {
        logger.info("Creating the JCA context as part of UpdatePVsatRate initialization");
        // Get the JCALibrary instance.
        JCALibrary jca = JCALibrary.getInstance();
        ByteArrayInputStream bis = JCAConfigGen.generateJCAConfig(configService);
        DefaultConfigurationBuilder configBuilder = new DefaultConfigurationBuilder();
        Configuration configuration = configBuilder.build(bis);
        // Now, we'll create a context with this configuration.
        JCAContext = jca.createContext(configuration);
        logger.info("Successfully created the JCA context as part of UpdatePVsatRate initialization");

        connectedChannels = new ConcurrentHashMap<String, UpdatePVsatRate.UpdateValue>(totalpvcount);

        for (int i = 0; i < totalpvcount; i++) {
            // The "test" comes from a macro but we hardcode it here.
            String pvName = "test:sine_" + i;
            ConnCallback cb = new ConnCallback(i);
            Channel channel = JCAContext.createChannel(pvName, cb);
            cb.setChannel(channel);
        }

        eventGenerator.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        // Every second , we pick up "rate" number of connected PV's at random and update them to their
                        // next value.
                        System.out.print("Begin update");
                        Collection<UpdateValue> values = connectedChannels.values();
                        ArrayList<UpdateValue> shuffle = new ArrayList<UpdateValue>(values);
                        Collections.shuffle(shuffle);
                        for (int i = 0; i < rate && i < shuffle.size(); i++) {
                            UpdateValue pvupdate = shuffle.get(i);
                            pvupdate.scheduleUpdate();
                        }
                        System.out.println(" and end update");
                    }
                },
                10,
                1,
                TimeUnit.SECONDS);
    }

    public void processContextTasks() {
        while (true) {
            try {
                Runnable task = contextTasks.take();
                task.run();
                JCAContext.flushIO();
            } catch (Throwable t) {
                logger.error("Exception processing context tasks", t);
            }
        }
    }

    private class ConnCallback implements ConnectionListener {
        private Channel channel;
        private int pvindex;

        public ConnCallback(int pvindex) {
            this.pvindex = pvindex;
        }

        @Override
        public void connectionChanged(ConnectionEvent connEvent) {
            if (connEvent.isConnected()) {
                connectedChannels.putIfAbsent("test:sine_" + pvindex, new UpdateValue(channel, pvindex));
            } else {
                connectedChannels.remove("test:sine_" + pvindex);
            }
        }

        void setChannel(Channel channel) {
            this.channel = channel;
        }
    }

    private class UpdateValue {
        private Channel channel;
        private int pvindex;

        UpdateValue(Channel channel, int pvindex) {
            this.channel = channel;
            this.pvindex = pvindex;
        }

        public void scheduleUpdate() {
            contextTasks.add(new JCAPut(channel, pvindex));
        }
    }

    private class JCAPut implements Runnable {
        private Channel channel;
        private int pvindex;
        private int phase;

        JCAPut(Channel channel, int pvindex) {
            this.channel = channel;
            this.pvindex = pvindex;
            phase = (this.pvindex % 10) * 36;
        }

        @Override
        public void run() {
            try {
                double degrees = ((System.currentTimeMillis() / 1000) % 3600) * (360.0 / 3600.0);
                logger.debug("Curr for " + pvindex + " = " + (degrees + phase));
                double radians = (degrees + phase) * (Math.PI / 180.0);
                double value = Math.sin(radians);
                channel.put(value, new JCAPutListener());
            } catch (CAException ex) {
                logger.warn("Exception ", ex);
            }
        }
    }

    private class JCAPutListener implements PutListener {
        @Override
        public void putCompleted(PutEvent putEvent) {
            // No need to do anything yet
        }
    }
}
