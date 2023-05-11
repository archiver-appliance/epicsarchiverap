package org.epics.archiverappliance.retrieval.mimeresponses;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import com.jmatio.io.MatFileWriter;
import com.jmatio.types.MLArray;
import com.jmatio.types.MLChar;
import com.jmatio.types.MLDouble;
import com.jmatio.types.MLStructure;
import com.jmatio.types.MLUInt64;
import com.jmatio.types.MLUInt8;

/**
 * Generate a ".mat" matlab file
 * The response contains two objects, a header and a data object.
 * The header object is indexed by the string <code>header</code> and is a MLStructure with fields for pvname, start and end times etc.
 * The data object is indexed by the string <code>data</code> and is a MLStructure with these fields.
 * <ol>
 * <li><code>epochSeconds</code> - contains Java epoch seconds as a 1x1 uint64 array. The times are in UTC; so any conversion to local time needs to happen at the client.</li>
 * <li><code>values</code> - contains the values for the samples. All scalars come as a 1x1 double array. Waveforms come as a 1x<i>elementcount</i> double array where <i>elementcount</i> is the EPICS element count of the waveform.</li>
 * <li><code>nanos</code> - contains the nano second value of the EPICS record processing timestamp as a 1x1 uint64 array. Some installations embed the beam code/pulse id into this field; however, this is done at the IOC  side and as far as this code are concerned, this is the nanoseconds.</li>
 * <li><code>isDST</code> - contains booleans that indicate if the time indicated by <code>epochSeconds</code> was in daylight savings time in the timezone of the server. This is an attempt to get around the deficiencies in Matlab w.r.t timezones.</li>
 * </ol>
 * 
 * If needed, we can add separate fields for status and severity.
 * 
 * Owing to the column-major nature, this response consumes a lot of memory.
 * If we are serving large datasets using Matlab responses, we should increase the heap size for the retrieval war to large values.
 * 
 * @author mshankar
 *
 */
public class MatlabResponse implements MimeResponse {
	private static Logger logger = LogManager.getLogger(MatlabResponse.class.getName());
	private MLStructure headerStruct = new MLStructure("header", new int[] {1, 1});
	private MLStructure dataStruct = new MLStructure("data", new int[] {1, 1});
	private WritableByteChannel channel = null; 
	private ArrayListEventStream dest = null;
	private boolean typesSet = false;
	private ArchDBRTypes dbrType = ArchDBRTypes.DBR_SCALAR_DOUBLE;
	private int elementCount = 1;
	
	@Override
	public void consumeEvent(Event e) throws Exception {
		dest.add(e.makeClone());
		if(!typesSet) {
			typesSet = true;
			dbrType = e.getDBRType();
			elementCount = e.getSampleValue().getElementCount();
		}
	}

	@Override
	public void setOutputStream(OutputStream os) {
		channel = Channels.newChannel(os);
	}

	@Override
	public void processingPV(BasicContext retrievalContext, String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
		headerStruct.setField("source", new MLChar("source", "Archiver appliance"));
		headerStruct.setField("pvName", new MLChar("pvName", pv));
		headerStruct.setField("from", new MLChar("from", TimeUtils.convertToISO8601String(start)));
		headerStruct.setField("to", new MLChar("to", TimeUtils.convertToISO8601String(end)));
		
		dest = new ArrayListEventStream(0, new RemotableEventStreamDesc(streamDesc.getArchDBRType(), pv, TimeUtils.computeYearForEpochSeconds(TimeUtils.convertToEpochSeconds(start))));
	}

	@Override
	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}

	@SuppressWarnings("unused")
	@Override
	public void close() {
		try { 
			dataStruct.setField("epochSeconds", new GenTimeArray().generateColumn(dest));
			dataStruct.setField("values", new GenValueArray().generateColumn(dest));
			dataStruct.setField("nanos", new GenNanosArray().generateColumn(dest));
			dataStruct.setField("isDST", new GenisDSTArray().generateColumn(dest));
			LinkedList<MLArray> dataList = new LinkedList<MLArray>();
			dataList.add(headerStruct);
			dataList.add(dataStruct);
			new MatFileWriter(channel, dataList);
		} catch (IOException ex) { 
			if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
				// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
				// This is thrown if the client closes abruptly - that is, if Matlab crashes.
				logger.debug("Exception generating matlab file", ex);
			} else { 
				logger.error("Exception generating matlab file", ex);
			}
		} finally { 
			try { channel.close(); channel = null; } catch (Exception ex) { } 
		}
	}

	private interface GenMLArray { 
		public MLArray generateColumn(ArrayListEventStream dest);
	}
	
	private class GenTimeArray implements GenMLArray { 
		public MLArray generateColumn(ArrayListEventStream dest) { 
			MLUInt64 ret = new MLUInt64("epochSeconds", new int[] {dest.size(), 1} );
			int i = 0;
			for(Event e : dest) {
				ret.set(e.getEpochSeconds(), i++);
			}
			return ret;
		}
	}
	
	private class GenisDSTArray implements GenMLArray { 
		public MLArray generateColumn(ArrayListEventStream dest) { 
			MLUInt8 ret = new MLUInt8("isDST", new int[] {dest.size(), 1} );
			int i = 0;
			for(Event e : dest) {
				ret.set(TimeUtils.isDST(e.getEventTimeStamp()) ? (byte) 1 : (byte) 0, i++);
			}
			return ret;
		}
	}


	private class GenValueArray implements GenMLArray { 
		public MLArray generateColumn(ArrayListEventStream dest) { 
			if(dbrType.isWaveForm()) {
				MLDouble ret = new MLDouble("values", new int[] {dest.size(), elementCount} );
				int i = 0;
				for(Event e : dest) {
					SampleValue sampleValue = e.getSampleValue();
					for(int col = 0; col < sampleValue.getElementCount(); col++) {
						ret.set(sampleValue.getValue(col).doubleValue(), i, col);
					}
					i++;
				}
				return ret;
			} else {
				MLDouble ret = new MLDouble("values", new int[] {dest.size(), 1} );
				int i = 0;
				for(Event e : dest) {
					ret.set(e.getSampleValue().getValue().doubleValue(), i++);
				}
				return ret;
			}
		}
	}

	private class GenNanosArray implements GenMLArray { 
		public MLArray generateColumn(ArrayListEventStream dest) { 
			MLUInt64 ret = new MLUInt64("nanos", new int[] {dest.size(), 1} );
			int i = 0;
			for(Event e : dest) {
				ret.set((long) e.getEventTimeStamp().getNanos(), i++);
			}
			return ret;
		}
	}

	@Override
	public HashMap<String, String> getExtraHeaders() {
		return null;
	}

}
