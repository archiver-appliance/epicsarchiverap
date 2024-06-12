package org.epics.archiverappliance.common;

import java.io.IOException;
import java.time.Instant;
import java.time.Period;
import java.util.function.Predicate;

import org.epics.archiverappliance.Event;

public interface BiDirectionalIterable {
	public enum IterationDirection {
		FORWARDS,
		BACKWARDS
	};

	/**
	 * Generic method to iterate over the data for specified PV.
	 * We provide a time to start the iteration at ( inclusive ) and a direction and a Predicate.
	 * The plugin then iterates ( forwards or backwards ) and calls the Predicate for each sample.
	 * The iteration stops when the Predicate returns false or when we run out of samples.
	 * Iteration also stops when we get an exception. 
	 * So this mode of traversal is vulnerable to corruption in the data.
	 * We may relax this constraint later by providing a optional bool to ignore exceptions.
	 * @param context  &emsp;
	 * @param pvName The PV name
     * @param startAtTime Start the iteration at this time.
     * If going forwards, this is the first sample on or before the specified time. 
     * If going backwards, this is the first sample on or after the specified time.
     * @param thePredicate - the Predicate to apply 
     * @param direction - Forwards or backwards
	 * @param searchPeriod - An estimate on the amount of time we want to iterate.
	 * This is used to determine the appropriate chunks containing data at a very high level and thus to limit the search.
	 * The predicate itself is the one that controls when the iteration terminates.
	 * So, for example, you could specify a searchPeriod of 1 year but stop the iteration after 1 minute
	 * @throws IOException  &emsp;
	 */
	public void iterate(BasicContext context, String pvName, Instant startAtTime, Predicate<Event> thePredicate, IterationDirection direction, Period searchPeriod) throws IOException;
}
