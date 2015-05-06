package org.epics.archiverappliance.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * As part of archiver.names, the ChannelArchiver XMLRPC server also returns the first and last timestamp.
 * We store the start and end epoch seconds in addition to the ServerInfo in instances of this class.
 * This is typically used to sort the servers in reverse order of their first and last epoch seconds.
 * This gives up a poor mans implementation of Bob's traffic cop index; though the memory consumption is probably somewhat excessive.
 * @author mshankar
 *
 */
public class ChannelArchiverDataServerPVInfo implements Serializable {
	private static final long serialVersionUID = -1423395893486840642L;
	private ChannelArchiverDataServerInfo serverInfo;
	private long startSec;
	private long endSec;
	public ChannelArchiverDataServerPVInfo(ChannelArchiverDataServerInfo serverInfo, long startSec, long endSec) {
		this.serverInfo = serverInfo;
		this.startSec = startSec;
		this.endSec = endSec;
	}
	
	public static void sortServersBasedOnStartAndEndSecs(List<ChannelArchiverDataServerPVInfo> caPVInfos) { 
		Collections.sort(caPVInfos, new Comparator<ChannelArchiverDataServerPVInfo>() {
			@Override
			public int compare(ChannelArchiverDataServerPVInfo o1, ChannelArchiverDataServerPVInfo o2) {
				if(o1.endSec == o2.endSec) { 
					return 0;
				} else if(o1.endSec > o2.endSec) { 
					return -1;
				} else { 
					return 1;
				}
			}
		});
	}

	public ChannelArchiverDataServerInfo getServerInfo() {
		return serverInfo;
	}

	public long getStartSec() {
		return startSec;
	}

	public long getEndSec() {
		return endSec;
	}

	@Override
	public String toString() {
		return serverInfo.toString();
	}

}
