## SCAN vs MONITOR

Both SCAN and MONITOR use CA monitors, but they differ in how samples
are buffered.

**MONITOR** estimates the number of samples based on the PV's sampling
period, allocates a buffer to hold that many samples, and fills it up.
The buffer capacity is computed as:

```java
int buffer_capacity = ((int) Math.round(Math.max((write_period/pvSamplingPeriod)*sampleBufferCapacityAdjustment, 1.0))) + 1;
```

- `pvSamplingPeriod` — the `sampling_period` from the PV's PVTypeInfo
- `write_period` — the engine's write period from `archappl.properties` (`secondsToBuffer`); defaults to 10 seconds
- `sampleBufferCapacityAdjustment` — a system-wide buffer-size adjustment set in `archappl.properties`

For example, if `write_period` is 10 seconds and `pvSamplingPeriod` is
1 second, the buffer holds 11 samples. For a PV changing faster than 1
Hz, you will see 11 samples, a gap where the buffer overflows, then 11
more samples on the next buffer switch.

**SCAN** updates one slot at whatever rate is sent from the IOC, then a
separate thread reads that slot every `pvSamplingPeriod` seconds and
writes it to the buffer. This gives exactly one sample per
`pvSamplingPeriod` regardless of the IOC update rate, at the cost of
potentially missing intermediate values.
