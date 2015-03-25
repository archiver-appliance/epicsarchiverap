package org.epics.archiverappliance.etl.common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ConfigService;

public class ETLGatingState {
	protected static Logger logger = Logger.getLogger(ETLGatingState.class.getName());

	protected final ConfigService m_configService;
	private final Object m_lock = new Object();
	private final HashMap<String, ETLGatingScope> m_scopes = new HashMap<String, ETLGatingScope>();

	/**
	* Creates the gating state instance.
	*/
	public ETLGatingState(ConfigService configService)
	{
		m_configService = configService;
	}

	/**
	* Remember that samples from a specific time interval should pass the ETL gate.
	*
	* @param scopeName - The name of the ETL gating scope to work in. Nonexisting scopes
	*                    are created automatically.
	* @param startMillis - The interval start time in 1970-epoch milliseconds (inclusive).
	* @param endMillis - The interval end time in 1970-epoch milliseconds (exclusive).
	*/
	public void keepTimeInterval(String scopeName, long startMillis, long endMillis) {
		synchronized (m_lock) {
			ETLGatingScope scope = getScope(scopeName);
			scope.keepTimeInterval(startMillis, endMillis);
		}
	}

	/**
	* Determines whether samples from a specific time interval should pass the ETL gate.
	*
	* @param scopeName - The name of the ETL gating scope to work in. Nonexisting scopes
	*                    are created automatically.
	* @param startMillis - The interval start time in 1970-epoch milliseconds (inclusive).
	* @param endMillis - The interval end time in 1970-epoch milliseconds (exclusive).
	*/
	public boolean shouldIntervalBeKept(String scopeName, long startMillis, long endMillis) {
		synchronized (m_lock) {
			ETLGatingScope scope = getScope(scopeName);
			return scope.shouldIntervalBeKept(startMillis, endMillis);
		}
	}

	private ETLGatingScope getScope(String scopeName) {
		if (!m_scopes.containsKey(scopeName)) {
			logger.info("Creating new ETL gating scope with name = " + scopeName);
			m_scopes.put(scopeName, new ETLGatingScope(this, scopeName));
		}
		return m_scopes.get(scopeName);
	}
}

class ETLGatingScope {
	private static int DEFAULT_MAX_INTERVALS = 100;

	private final int m_maxIntervals;

	// Here we keep the time intervals which define the data that should pass the ETL gate.
	// They are considered as half-open [) intervals, with the invariant that they are
	// mutually strictly non-overlapping and non-touching. The TreeSet keeps them in their
	// natural order.
	private final TreeSet<ETLGatingInterval> m_intervals = new TreeSet<ETLGatingInterval>();

	public ETLGatingScope(ETLGatingState gatingState, String name)
	{
		String maxIntervalsProperty = "org.epics.archiverappliance.etl.common.GatingMaxIntervals_" + name;
		m_maxIntervals = Integer.parseInt(gatingState.m_configService.getInstallationProperties().getProperty(maxIntervalsProperty, Integer.toString(DEFAULT_MAX_INTERVALS)));
	}

	public void keepTimeInterval(long start, long end) {
		// Disallow non-positive intervals.
		if (end <= start) {
			return;
		}
		
		// The merging algorithm is as follows: we remove all existing intervals
		// which overlap with or touch the request interval, and replace them with
		// a new interval which is a union of all removed intervals and the request
		// interval. This union interval is defined by the minimum of the start
		// points and the maximum of end points.
		ETLGatingInterval requestInterval = new ETLGatingInterval(start, end);
		long merged_start = requestInterval.start;
		long merged_end = requestInterval.end;
		Iterator<ETLGatingInterval> iter = m_intervals.iterator();
		while (iter.hasNext()) {
			ETLGatingInterval interval = iter.next();
			if (interval.overlapsWithOrTouches(requestInterval)) {
				iter.remove();
				merged_start = Math.min(merged_start, interval.start);
				merged_end = Math.max(merged_end, interval.end);
			}
		}
		m_intervals.add(new ETLGatingInterval(merged_start, merged_end));
		
		// Possibly delete intervals from the beginning to keep the interval
		// list size within the bound.
		while (m_intervals.size() > m_maxIntervals) {
			m_intervals.pollFirst();
		}
	}

	public boolean shouldIntervalBeKept(long start, long end) {
		// The interval should be kept if it overlaps with any of the
		// intervals in the list.
		ETLGatingInterval queryInterval = new ETLGatingInterval(start, end);
		for (ETLGatingInterval interval : m_intervals) {
			if (interval.overlapsWith(queryInterval)) {
				return true;
			}
		}
		return false;
	}
}

class ETLGatingInterval implements Comparable<ETLGatingInterval> {
	public final long start;
	public final long end;

	public ETLGatingInterval(long start, long end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public int compareTo(ETLGatingInterval other) {
		Long a = new Long(this.start);
		Long b = new Long(other.start);
		return a.compareTo(b);
	}

	public boolean overlapsWithOrTouches(ETLGatingInterval other) {
		return (other.end >= this.start && other.start <= this.end);
	}

	public boolean overlapsWith(ETLGatingInterval other) {
		return (other.end > this.start && other.start < this.end);
	}
}
