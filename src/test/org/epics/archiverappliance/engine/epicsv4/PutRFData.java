package org.epics.archiverappliance.engine.epicsv4;

import org.apache.log4j.Logger;
import org.epics.ca.client.Channel;
import org.epics.ca.client.Channel.ConnectionState;
import org.epics.ca.client.ChannelAccess;
import org.epics.ca.client.ChannelAccessFactory;
import org.epics.ca.client.ChannelProvider;
import org.epics.ca.client.ChannelPut;
import org.epics.ca.client.ChannelPutRequester;
import org.epics.ca.client.ChannelRequester;
import org.epics.ca.client.CreateRequestFactory;
import org.epics.pvData.misc.BitSet;
import org.epics.pvData.pv.MessageType;
import org.epics.pvData.pv.PVDouble;
import org.epics.pvData.pv.PVInt;
import org.epics.pvData.pv.PVLong;
import org.epics.pvData.pv.PVStructure;
import org.epics.pvData.pv.Status;

public class PutRFData implements ChannelRequester, ChannelPutRequester{
	private static Logger logger = Logger.getLogger(PutRFData.class.getName());
	@Override
	public String getRequesterName() {
		
		return this.getClass().getName();
	}

	@Override
	public void message(String arg0, MessageType arg1) {
		
		
	}

	@Override
	public void channelPutConnect(Status status, ChannelPut channelPut,
			PVStructure pvstructure, BitSet bitset) {
		
		// PVDouble val = this.pvStructure.getDoubleField("value");
		if(status.isSuccess()){
			
			//System.out.println("connect success");
		//	System.out.println(pvstructure);
		PVStructure pvStructure2 =pvstructure.getStructureField("value");
		PVStructure timeStamppvStructure =pvstructure.getStructureField("timeStamp");
		
		//while(true)
		{
		channelPut.lock();
		PVDouble phase = pvStructure2.getDoubleField("phase");
		 bitset.set(phase.getFieldOffset());
		phase.put(Math.random()*100);
		PVDouble amplitude = pvStructure2.getDoubleField("amplitude");
		bitset.set(amplitude.getFieldOffset());
		amplitude.put(Math.random()*100);
		PVLong  secondsPastEpochPVLong= timeStamppvStructure.getLongField("secondsPastEpoch");
		 bitset.set(secondsPastEpochPVLong.getFieldOffset());
		secondsPastEpochPVLong.put(System.currentTimeMillis()/1000);
		
		PVInt  nanoSecondsPVInt= timeStamppvStructure.getIntField("nanoSeconds");
		nanoSecondsPVInt.put((int)((System.currentTimeMillis()%1000)*1000000));
		channelPut.unlock();
		channelPut.put(true);
	/*	try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			
			logger.error("Exception", e);
		}
		*/
		}
		}
		else {
			//System.out.println("connect unsuccess");
		}
	}

	@Override
	public void getDone(Status arg0) {
		
		
	}

	@Override
	public void putDone(Status status) {
		
	/*	if(status.isSuccess())System.out.println("put success");
		else
		{
			System.out.println("put unsuccess");
		}
		
	*/	
	}

	@Override
	public void channelCreated(Status arg0, Channel arg1) {
		
		
	}

	@Override
	public void channelStateChange(Channel arg0, ConnectionState arg1) {
	
		
	}
	
	public void putRF()
	{
		 try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				
				logger.error("Exception", e);
			}
		while(true)
		{
		 org.epics.ca.ClientFactory.start();

		    String providerName = "pvAccess";
		    PutRFData  request=new PutRFData();
		
			ChannelAccess channelAccess = ChannelAccessFactory.getChannelAccess();
		    ChannelProvider channelProvider = channelAccess.getProvider(providerName);
		    Channel channel = channelProvider.createChannel("rf", request, ChannelProvider.PRIORITY_DEFAULT);
		    
		    PVStructure pvRequest = CreateRequestFactory.createRequest("record[queueSize=" + 1 + "]" +
	   	    		"field(value{value.phase,value.amplitude},timeStamp)",request);
	   	    //record[] field(value{value.phase,value.amplitude})
		    
		    channel.createChannelPut(request, pvRequest);
		    
		    try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				
				logger.error("Exception", e);
			}
		      
		      org.epics.ca.ClientFactory.stop();
		}
	}
	
	
	public static void main(String agrs[])
	{
		new PutRFData().putRF();
	}

}
