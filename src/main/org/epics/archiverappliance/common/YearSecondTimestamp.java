/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

/**
 * This is a version of a timestamp that breaks down time into a short year, a signed int secondsintoyear and a signed int nanos
 * This is the format that is used internally in the protocol buffers storage plugin.
 * When storing files partitioned by years, the year is dropped from the event, saving two bytes of storage per event.
 * When reconstructing such files, we determine the year from the file name and add it back in.
 * @author mshankar
 *
 */
public class YearSecondTimestamp implements Comparable<YearSecondTimestamp> {

    private short year;
    private int secondsintoyear;
    private int nanos;

    public YearSecondTimestamp(short year, int secondsintoyear, int nanos) {
        this.year = year;
        this.secondsintoyear = secondsintoyear;
        this.nanos = nanos;
    }

    public short getYear() {
        return year;
    }

    public void setYear(short year) {
        this.year = year;
    }

    public int getSecondsintoyear() {
        return secondsintoyear;
    }

    public void setSecondsintoyear(int secondsintoyear) {
        this.secondsintoyear = secondsintoyear;
    }

    public int getNano() {
        return nanos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YearSecondTimestamp that = (YearSecondTimestamp) o;

        if (year != that.year) return false;
        if (secondsintoyear != that.secondsintoyear) return false;
        return nanos == that.nanos;
    }

    @Override
    public int hashCode() {
        int result = year;
        result = 31 * result + secondsintoyear;
        result = 31 * result + nanos;
        return result;
    }

    public void setNanos(int nanos) {
        this.nanos = nanos;
    }

    @Override
    public String toString() {
        return "YearSecondTimestamp{datetime="
                + TimeUtils.convertFromYearSecondTimestamp(this) + ", year="
                + year + ", secondsintoyear="
                + secondsintoyear + ", nanos="
                + nanos + '}';
    }

    @Override
    public int compareTo(YearSecondTimestamp other) {
        if (this.year == other.year) {
            if (this.secondsintoyear == other.secondsintoyear) {
                if (this.nanos == other.nanos) {
                    return 0;
                } else {
                    return this.nanos - other.nanos;
                }
            } else {
                return this.secondsintoyear - other.secondsintoyear;
            }
        } else {
            return this.year - other.year;
        }
    }
}
