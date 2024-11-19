package org.epics.archiverappliance.common;

import org.epics.archiverappliance.Event;

import java.io.IOException;
import java.time.Instant;
import java.time.Period;

public interface DataAtTime {

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
     * @param atTime Time looking for.
     * @param startAtTime Time to start looking from.
     * @param searchPeriod - An estimate on the amount of time we want to iterate.
     * This is used to determine the appropriate chunks containing data at a very high level and thus to limit the search.
     * The predicate itself is the one that controls when the iteration terminates.
     * So, for example, you could specify a searchPeriod of 1 year but stop the iteration after 1 minute
     * @throws IOException  &emsp;
     */
    Event dataAtTime(
            BasicContext context,
            String pvName,
            Instant atTime,
            Instant startAtTime,
            Period searchPeriod,
            BiDirectionalIterable.IterationDirection iterationDirection)
            throws IOException;
}
