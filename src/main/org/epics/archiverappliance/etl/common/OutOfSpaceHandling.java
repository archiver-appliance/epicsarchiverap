package org.epics.archiverappliance.etl.common;


/**
 * How should ETL deal with running out of space in the dest? 
 * Should we delete the source streams? This would mean that we would lose data. 
 * If we do not delete the source streams, then the source itself will start filling up and then the previous source and so on.
 * Eventually the engine write thread will have to check space availability before each write and that would be very time consuming.
 * 
 * Some choices; this can be set in archappl.properties
 * <ol>
 * <li>Fill up the source; aka, skip ETL this time around and hope the sysadmin gets to freeing space on the dest before the next round.</li>
 * <li>Delete the source streams; in extreme situations like this; deleting data now is not much different from deleting data later.</li>
 * <li>Delete the source streams only if the source is the first destination. This should theoretically let you fill up all the sources without having to have the engine check on each and every write. </li>
 * </ol>
 * 
 * At an initial glance, the DELETE_SRC_STREAMS_IF_FIRST_DEST_WHEN_OUT_OF_SPACE seems to make the most sense; so that's the default.
 * @author mshankar
 *
 */
public enum OutOfSpaceHandling {
	SKIP_ETL_WHEN_OUT_OF_SPACE,
	DELETE_SRC_STREAMS_WHEN_OUT_OF_SPACE,
	DELETE_SRC_STREAMS_IF_FIRST_DEST_WHEN_OUT_OF_SPACE;
}
