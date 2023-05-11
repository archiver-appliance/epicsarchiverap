package org.epics.archiverappliance.engine.V4;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.pvaccess.client.Channel;
import org.epics.pvaccess.client.Channel.ConnectionState;
import org.epics.pvaccess.client.ChannelProvider;
import org.epics.pvaccess.client.ChannelProviderRegistryFactory;
import org.epics.pvaccess.client.ChannelRequester;
import org.epics.pvdata.copy.CreateRequest;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.monitor.Monitor;
import org.epics.pvdata.monitor.MonitorElement;
import org.epics.pvdata.monitor.MonitorRequester;
import org.epics.pvdata.pv.MessageType;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Structure;

/**
 * Sample V4 client to explore the API a bit; feel free to delete this file if the API's change
 * @author mshankar
 *
 */
public class SampleV4Client implements ChannelRequester, MonitorRequester {
	private static final Logger logger = LogManager.getLogger(SampleV4Client.class);
	private static ChannelProvider channelProvider;
	
	private Channel channel;
	List<String> fieldNames = new LinkedList<String>();
	
	public SampleV4Client(String pvName) { 
        channel = channelProvider.createChannel(pvName, this, ChannelProvider.PRIORITY_DEFAULT);
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) {
        org.epics.pvaccess.ClientFactory.start();
        logger.info("Registered the pvAccess client factory.");
        channelProvider = ChannelProviderRegistryFactory.getChannelProviderRegistry().getProvider(org.epics.pvaccess.ClientFactory.PROVIDER_NAME);

        for(String providerName : ChannelProviderRegistryFactory.getChannelProviderRegistry().getProviderNames()) {
                logger.info("PVAccess Channel provider " + providerName);
        }

        for(String pvName : args) { 
			new SampleV4Client(pvName);
		}
	}

	@Override
	public String getRequesterName() {
		return "SampleV4Client";
	}

	@Override
	public void message(String message, MessageType arg1) {
		logger.debug(message);
	}

	@Override
	public void channelCreated(Status arg0, Channel arg1) {
	}

	@Override
	public void channelStateChange(Channel channelChangingState, ConnectionState connectionStatus) {
		if (connectionStatus == ConnectionState.CONNECTED) {
			logger.info("channelStateChange:connected " + channelChangingState.getChannelName());
			PVStructure pvRequest = CreateRequest.create().createRequest("field()"); 
			channel.createMonitor(this, pvRequest);
		}
	}
	
	private void createFieldIdToNameIndex(List<String> fieldNames, Structure structure, String structureName) {
		fieldNames.add(structureName);
		for(String fieldName : structure.getFieldNames()) { 
			switch(structure.getField(fieldName).getType()) { 
	            case structure: {
	            	createFieldIdToNameIndex(fieldNames, (Structure) structure.getField(fieldName), structureName.isEmpty() ? fieldName : (structureName + "." + fieldName));
	            	continue;
	            }
	            case scalar:
	            case scalarArray:
	            case structureArray:
	            case union:
	            case unionArray:
	            default: { 
	            	fieldNames.add(structureName.isEmpty() ? fieldName : (structureName + "." + fieldName));
	            	continue;
	            }
			}
		}
	}

	@Override
	public void monitorConnect(Status arg0, Monitor channelMonitor, Structure structure) {
		createFieldIdToNameIndex(fieldNames, structure, "");
		for(int i = 0; i < fieldNames.size(); i++) {
			logger.info("Field " + i + " = " + fieldNames.get(i));
		}
		channelMonitor.start();
	}

	@Override
	public void monitorEvent(Monitor monitor) {
		MonitorElement monitorElement = null;
		try {
			monitorElement = monitor.poll();
			while (monitorElement != null)  { 
				try { 
					logger.info("Obtained monitor event for pv " + channel.getChannelName());
					// PVStructure totalPVStructure = monitorElement.getPVStructure();
					BitSet changedBits = monitorElement.getChangedBitSet();
					logger.info("Obtained monitor event for pv " + channel.getChannelName() + " Changed bits: " + changedBits.toString());
					for (int i = changedBits.nextSetBit(0); i >= 0; i = changedBits.nextSetBit(i+1)) {
						logger.info("Field has changed: " + fieldNames.get(i));
					}
				} finally { 
					monitor.release(monitorElement);
				}
				monitorElement = monitor.poll();
			}
		} catch(Exception ex) { 
			logger.error("Exception processing monitor event", ex);
		}
	}

	@Override
	public void unlisten(Monitor arg0) {
	}

}
