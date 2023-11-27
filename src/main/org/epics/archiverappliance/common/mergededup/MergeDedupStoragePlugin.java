package org.epics.archiverappliance.common.mergededup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.StoragePluginURLParser;
import org.epics.archiverappliance.etl.ConversionFunction;
import org.epics.archiverappliance.etl.ETLContext;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.ETLInfo;
import org.epics.archiverappliance.etl.ETLSource;
import org.epics.archiverappliance.etl.ETLStreamCreator;
import org.epics.archiverappliance.etl.StorageMetrics;
import org.epics.archiverappliance.etl.StorageMetricsContext;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.DefaultRawPostProcessor;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;
import org.epics.archiverappliance.utils.ui.URIUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * The MergeDedupStoragePlugin is primarily meant for achieving a small amount of failover in the archiving of a PV.
 * The scheme is to have another appliance also archive the same PV and then merge the data from that appliance into this one during ETL. 
 * This appliance is the <b><i>dest</i></b> appliance; this is where the data is merged into. 
 * The <b><i>other</i></b> appliance is an independent EPICS archiver appliance archiving the same PV; that is, it is not part of this cluster of appliances.
 * There are no special requirements for the <i>other</i> appliance other than that it should archive the same PV (and of course, a reasonably similar version to this one).
 * No calls are made to the <i>other</i> appliance to cleanup any data after consolidation; so, for convenience, the <i>other</i> appliance can be configured with a BlackholeStoragePlugin to automatically delete data after a certain time.
 *   
 * The MergeDedupStoragePlugin has two StoragePlugins parameters in its configuration.
 * <ul>
 * <li>The <code>dest</code> parameter configures the data store in this appliance.</li>
 * <li>The <code>other</code> parameter points to the backup appliance using the <code>data_retrieval_url</code> of the <i>other</i> appliance.
 * </li>
 * </ul>
 * The <i>other</i> appliance also acts an ETL gating point; that is, we do not move the data out of this data store unless we get a valid response from the <i>other</i> appliance when we fetch data during ETL. 
 * So, ETL for this PV in this appliance will stop if you bring down the <i>other</i> appliance. It will automatically resume and continue from where it left off once you bring the <i>other</i> appliance back up.
 * Note that data is merged into this appliance only during ETL. So, if you have data that is outside the ETL window ( some complex combination of pause resume etc), you'll have to manually merge the data in.
 * This can be done using the <code>mergeInData</code> BPL call; the PV needs to be paused for this purpose.
 * 
 * <div>As an example, assume that you wish to merge the data when you move data from the MTS to LTS; then you define your MTS in this appliance as a MergeDedupStoragePlugin.
 * If, for example. 
 * <ul>
 * <li>you use <code>pb://localhost?name=MTS&amp;rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&amp;partitionGranularity=PARTITION_DAY&amp;hold=2&amp;gather=1</code> as your regular MTS.</li>
 * <li>the <code>data_retrieval_url</code> for the <i>other</i> appliance is <code>http://localhost:17669/retrieval</code></li>
 * </ul>
 * then, we'd define the MTS for this PV for this appliance as <pre><code><b>merge://</b>localhost?<b>name</b>=MTS
 *&amp;<b>dest</b>=pb%3A%2F%2Flocalhost%3Fname%3DMTS%26rootFolder%3D%24%7BARCHAPPL_MEDIUM_TERM_FOLDER%7D%26partitionGranularity%3DPARTITION_DAY%26hold%3D2%26gather%3D1
 *&amp;<b>other</b>=pbraw%3A%2F%2Flocalhost%3Fname%3DMTS%26rawURL%3Dhttp%253A%252F%252Flocalhost%253A17669%252Fretrieval%252Fdata%252FgetData.raw</code></pre>
 * where both the <b>dest</b> and <b>other</b> parameters are URL encoded.
 * <ul>
 * <li><b>dest</b> is URL encoded version of <code>pb://localhost?name=MTS&amp;rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&amp;partitionGranularity=PARTITION_DAY&amp;hold=2&amp;gather=1</code></li>
 * <li><b>other</b> is URL encoded version of <code>pbraw://localhost?name=MTS&amp;<b>rawURL</b>=http%3A%2F%2Flocalhost%3A17669%2Fretrieval%2Fdata%2FgetData.raw</code>
 * <ul>
 * <li>where the <b>rawURL</b> is the URL encoded version of <code>http://localhost:17669/retrieval/data/getData.raw</code></li>
 * </ul>
 * </li>
 * </ul> 
 * </div>
 * 
 * <div>From an implementation perspective, this can be understood as a plugin that delegates almost all calls to the <code>dest</code> plugin. 
 * Calls that fetch data out of this plugin are merged/deduped with the <code>other</code> plugin.</div>
 * @author mshankar
 *
 */
public class MergeDedupStoragePlugin implements StoragePlugin, ETLSource, ETLDest, StorageMetrics {
	private static Logger logger = LogManager.getLogger(MergeDedupStoragePlugin.class.getName());
	private String name;
	private StoragePlugin dest;
	private StoragePlugin other;
	private String desc = MergeDedupStoragePlugin.class.getName();
	private ConfigService configService;
	

	@Override
    public List<Callable<EventStream>> getDataForPV(BasicContext context, String pvName, Instant startTime,
                                                    Instant endTime, PostProcessor postProcessor) throws IOException {
		List<Callable<EventStream>> destStrms = dest.getDataForPV(context, pvName, startTime, endTime, postProcessor);
		List<Callable<EventStream>> othrStrms = other.getDataForPV(context, pvName, startTime, endTime, postProcessor);
		PVTypeInfo info = configService.getTypeInfoForPV(pvName);
		if(info == null) {
			throw new IOException("Cannot find PVTypeInfo for " + pvName);
		}
		return CallableEventStream.makeOneStreamCallableList(new MergeDedupWithCallablesEventStream(destStrms, othrStrms), postProcessor, true);
	}
	
	@Override
    public List<ETLInfo> getETLStreams(String pv, Instant currentTime, ETLContext context) throws IOException {
		List<ETLInfo> infos = ((ETLSource)dest).getETLStreams(pv, currentTime, context);
		if(infos == null) {
			logger.error("No ETL streams from " + dest.getDescription());
			return infos;
		}
		for(ETLInfo info : infos) {
            Instant startTime = TimeUtils.getPreviousPartitionLastSecond(info.getFirstEvent().getEventTimeStamp(), info.getGranularity());
            Instant endTime = TimeUtils.getNextPartitionFirstSecond(info.getFirstEvent().getEventTimeStamp(), info.getGranularity());
			info.setStrmCreator(new MergeStreamCreator(info.getStrmCreator(), info.getPvName(), startTime, endTime));
		}
		return infos;
	}
	
	@Override
	public Event getFirstKnownEvent(BasicContext context, String pvName) throws IOException {
		Event destE = dest.getFirstKnownEvent(context, pvName);
		Event otherE = other.getFirstKnownEvent(context, pvName);
		if (destE != null && otherE != null) {
            if (destE.getEventTimeStamp().isBefore(otherE.getEventTimeStamp())) {
				return destE;
			} else {
				return otherE;
			}
		} else if (destE != null) {
			return destE;
		} else {
			return otherE;
		}
	}

	@Override
	public Event getLastKnownEvent(BasicContext context, String pvName) throws IOException {
		Event destE = dest.getLastKnownEvent(context, pvName);
		Event otherE = other.getLastKnownEvent(context, pvName);
		if (destE != null && otherE != null) {
            if (destE.getEventTimeStamp().isAfter(otherE.getEventTimeStamp())) {
				return destE;
			} else {
				return otherE;
			}
		} else if (destE != null) {
			return destE;
		} else {
			return otherE;
		}
	}

	@Override
    public int appendData(BasicContext context, String pvName, EventStream stream) throws IOException {
		return dest.appendData(context, pvName, stream);
	}

	class MergeStreamCreator implements ETLStreamCreator {
		ETLStreamCreator strmCreator;
        Instant startTime, endTime;
		String pvName;

        MergeStreamCreator(ETLStreamCreator strc, String pvName, Instant sTime, Instant eTime) {
			this.strmCreator = strc;
            this.startTime = sTime;
            this.endTime = eTime;
			this.pvName = pvName;
		}

		@Override
		public EventStream getStream() throws IOException {
			try(BasicContext context = new BasicContext()) {
				List<Callable<EventStream>> othrStrms = other.getDataForPV(context, pvName, startTime, endTime, new DefaultRawPostProcessor());
				if(othrStrms == null || othrStrms.size() == 0) {
					throw new IOException("No data from other plugin; skipping ETL for now");
				}
				EventStream srcStream = strmCreator.getStream();
				logger.info("Merging " + srcStream.getDescription().getSource() + " with streams from " + other.getDescription());
				return new MergeDedupWithCallablesEventStream(CallableEventStream.makeOneStreamCallableList(srcStream), othrStrms);
			}
		}
	}

	@Override
	public long getTotalSpace(StorageMetricsContext storageMetricsContext) throws IOException {
		return ((StorageMetrics)dest).getTotalSpace(storageMetricsContext);
	}

	@Override
	public long getUsableSpace(StorageMetricsContext storageMetricsContext) throws IOException {
		return ((StorageMetrics)dest).getUsableSpace(storageMetricsContext);
	}

	@Override
	public long spaceConsumedByPV(String pvName) throws IOException {
		return ((StorageMetrics)dest).spaceConsumedByPV(pvName);
	}

	@Override
	public boolean prepareForNewPartition(String pvName, Event ev, ArchDBRTypes archDBRType, ETLContext context)
			throws IOException {
		return ((ETLDest)dest).prepareForNewPartition(pvName, ev, archDBRType, context);
	}

	@Override
	public boolean appendToETLAppendData(String pvName, EventStream stream, ETLContext context) throws IOException {
		return ((ETLDest)dest).appendToETLAppendData(pvName, stream, context);
	}

	@Override
	public boolean commitETLAppendData(String pvName, ETLContext context) throws IOException {
		return ((ETLDest)dest).commitETLAppendData(pvName, context);
	}

	@Override
	public boolean runPostProcessors(String pvName, ArchDBRTypes dbrtype, ETLContext context) throws IOException {
		return ((ETLDest)dest).runPostProcessors(pvName, dbrtype, context);
	}

	@Override
	public void markForDeletion(ETLInfo info, ETLContext context) {
		((ETLSource)dest).markForDeletion(info, context);
	}

	@Override
	public PartitionGranularity getPartitionGranularity() {
		return ((ETLSource)dest).getPartitionGranularity();
	}

	@Override
	public boolean consolidateOnShutdown() {
		return ((ETLSource)dest).consolidateOnShutdown();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getDescription() {
		return desc;
	}

	@Override
	public void initialize(String configURL, ConfigService configService) throws IOException {
		try {
			this.configService = configService;
			URI srcURI = new URI(configURL);
			HashMap<String, String> queryNVPairs = URIUtils.parseQueryString(srcURI);
			if(queryNVPairs.containsKey("name")) {
				name = queryNVPairs.get("name");
			} else {
				throw new IOException("Cannot initialize the plugin; this plugin implements the storage metrics API which needs an identity");
			}
			
			if(queryNVPairs.containsKey("dest")) {
				String destSpec = queryNVPairs.get("dest");
				this.dest = StoragePluginURLParser.parseStoragePlugin(destSpec, configService);
			} else {
				throw new IOException("Cannot initialize the plugin; please specify the dest plugin using the dest parameter");
			}

			if(queryNVPairs.containsKey("other")) {
				String otherSpec = queryNVPairs.get("other");
				this.other = StoragePluginURLParser.parseStoragePlugin(otherSpec, configService);
			} else {
				throw new IOException("Cannot initialize the plugin; please specify the other plugin using the other parameter");
			}
			
			if(this.dest instanceof ETLSource && this.dest instanceof ETLDest && this.dest instanceof StorageMetrics) {
				logger.debug("Dest plugin satisfies all constrints");
			} else {
				throw new IOException("Cannot initialize the plugin; please specify a dest plugin that implements the ETLDest, ETLSource and StorageMetrics interfaces");
			}

			this.desc = "MergeDedup plugin  - " + name + " with dest " + dest.getDescription() + " and other " + other.getDescription();
		} catch(URISyntaxException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void renamePV(BasicContext context, String oldName, String newName) throws IOException {
		dest.renamePV(context, oldName, newName);
		
	}

	@Override
	public void convert(BasicContext context, String pvName, ConversionFunction conversionFuntion) throws IOException {
		dest.convert(context, pvName, conversionFuntion);
	}

}
