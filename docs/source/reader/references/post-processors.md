# Post-processors

The EPICS archiver appliance has limited support for performing some
processing on the data during data retrieval. For most scientific data
processing purposes, a tool such as Matlab is a much better fit. To
retrieve data within Matlab, please see the [Matlab](../guides/matlab)
section.

To process the data during data retrieval, specify the operator during
the call to `getData`. For example, to get the `mean` of the PV
`test:pv:123`, ask for
`http://archiver.slac.stanford.edu/retrieval/data/getData.json?pv=mean(test%3Apv%3A123)`.
This mechanism should work within the ArchiveViewer as well. That is, if
you plot `mean(test:pv:123)` in the ArchiveViewer, the EPICS archiver
appliance applies the `mean` operator to the data for PV `test:pv:123`
before returning it to the client. To plot `test:pv:123` with the
`mean_3600` operator in the ArchiveViewer, plot
`mean_3600(test:pv:123)`.

The EPICS archiver appliance uses [Apache Commons Math](http://commons.apache.org/proper/commons-math/) for its data
processing. Many operators bin the data into bins whose sizes are
specified as part of the operator itself. For example, the `mean_3600`
operator bins the data into bins that are 3600 seconds wide. The default
binning interval is 900 seconds (15 minutes). The binning is done using
the [integer division](http://en.wikipedia.org/wiki/Euclidean_division)
operator. Two samples belong to the same bin if the quotient of the
sample's epoch seconds after integer division by the binning interval
is the same. For example, in the case of `mean_3600`, two samples `S1`
and `S2` belong to the same bin if
`S1.epochSeconds/3600 = S2.epochSeconds/3600` where `/` represents the
quotient after integer division. Samples belonging to the same bin are
gathered together and sent thru various statistics operators.

firstSample
: Returns the first sample in a bin. This is the default sparsification operator.

lastSample
: Returns the last sample in a bin.

firstFill
: Similar to the firstSample operator with the exception that we alter the timestamp to the middle of the bin and copy over the previous bin's value if a bin does not have any samples.

lastFill
: Similar to the firstFill operator with the exception that we use the last sample in the bin.

mean
: Returns the average value of a bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html) and is [SummaryStatistics.getMean()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getMean()>)

min
: Returns the minimum value in a bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html) and is [SummaryStatistics.getMin()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getMin()>)

max
: Returns the maximum value in a bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html) and is [SummaryStatistics.getMax()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getMax()>)

count
: Returns the number of samples in a bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html) and is [SummaryStatistics.getN()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getN()>)

ncount
: Returns the total number of samples in a selected time span.

nth
: Returns every n-th value.

median
: Returns the median value of a bin. This is computed using [DescriptiveStatistics](http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/stat/descriptive/DescriptiveStatistics.html) and is [DescriptiveStatistics.getPercentile(50)](<http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/stat/descriptive/DescriptiveStatistics.html#getPercentile(double)>)

std
: Returns the standard deviation of a bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html) and is [SummaryStatistics.getStandardDeviation()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getStandardDeviation()>)

jitter
: Returns the jitter (the standard deviation divided by the mean) of a bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html) and is [SummaryStatistics.getStandardDeviation()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getStandardDeviation()>)/[SummaryStatistics.getMean()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getMean()>)

ignoreflyers
: Ignores data that is more than the specified amount of std deviation from the mean in the bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html). It takes two arguments, the binning interval and the number of standard deviations (by default, 3.0). It filters the data and returns only those values which satisfy `Math.abs(val - SummaryStatistics.getMean()) <= numDeviations*SummaryStatistics.getStandardDeviation()`

flyers
: Opposite of ignoreflyers - only returns data that is more than the specified amount of std deviation from the mean in the bin.

variance
: Returns the variance of a bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html) and is [SummaryStatistics.getVariance()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getVariance()>)

popvariance
: Returns the population variance of a bin. This is computed using [SummaryStatistics](http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html) and is [SummaryStatistics.getPopulationVariance()](<http://commons.apache.org/proper/commons-math/javadocs/api-3.0/org/apache/commons/math3/stat/descriptive/SummaryStatistics.html#getPopulationVariance()>)

kurtosis
: Returns the kurtosis of a bin - Kurtosis is a measure of the peakedness. This is computed using [DescriptiveStatistics](http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/stat/descriptive/DescriptiveStatistics.html) and is [DescriptiveStatistics.getKurtosis()](<http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/stat/descriptive/DescriptiveStatistics.html#getKurtosis()>)

skewness
: Returns the skewness of a bin - Skewness is a measure of the asymmetry. This is computed using [DescriptiveStatistics](http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/stat/descriptive/DescriptiveStatistics.html) and is [DescriptiveStatistics.getSkewness()](<http://commons.apache.org/proper/commons-math/apidocs/org/apache/commons/math3/stat/descriptive/DescriptiveStatistics.html#getSkewness()>)

linear
: Implements the Linear arithmetic mean across an interval

loess
: Implements the Loess arithmetic mean across an interval

optimized
: Expects one parameter at initialization, which is the number of requested points.
If there are less samples in the time interval than requested (with a certain deadband), all samples
will be returned. If there are more samples than requested, the samples will be collected into bins.
Mean, std, min, max and count of each bin is calculated and returned as a single sample.

optimLastSample
: This differs from the `optimized` post processor in that if a bin is empty, instead of
repeating the last bin with samples, the bin uses the last value of the last recorded sample instead
(as mean, min and max; stddev is zero and number of samples is also zero).

caplotbinning
: Approx implementation of ChannelArchiver plotbinning

- If there is no sample for the time span of a bin, the bin remains empty.
- If there is one sample, it is placed in the bin.
- If there are two samples, they are placed in the bin.
- If there are more than two samples, the first and last one are placed in the bin.
  In addition, two artificial samples are created with a time stamp right
  between the first and last sample. One contains the minimum, the other
  the maximum of all raw samples whose time stamps fall into the bin. They
  are presented to the user in the sequence initial, minimum, maximum, final.

deadBand
: The intent is to mimic ADEL; this is principally targeted at decimation.

errorbar
: Similar to the mean operator; in addition, the std is passed in as an extra column
