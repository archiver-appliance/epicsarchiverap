/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Publishes the {@link Details} reports as Prometheus gauges.
 *
 * <p>Those reports are built for the metrics page, so the names are prose and the values are
 * formatted strings. This turns the name into a metric name and keeps the prose as the help text.
 * Entries whose value is not a number, such as a date or "N/A", have no Prometheus equivalent and
 * are skipped.
 *
 * @author caraxlr
 */
public class DetailsMetricsConverter {
    private static final Logger logger = LogManager.getLogger(DetailsMetricsConverter.class);

    private static final Pattern NOT_NAME_CHAR = Pattern.compile("[^a-zA-Z0-9]+");

    private DetailsMetricsConverter() {}

    /** Add every numeric entry as a gauge. */
    public static void addAll(PrometheusMetricsWriter writer, List<Map<String, String>> details) {
        // The client rejects a family holding two samples with the same labels, which would fail the
        // whole scrape rather than the one entry.
        Set<String> seen = new HashSet<>();
        for (Map<String, String> detail : details) {
            // An entry is the row of the metrics page: "name" is the prose label shown to a person,
            // "value" that row's reading, already formatted for display.
            String prose = detail.get("name");
            if (prose == null || prose.isBlank()) {
                continue;
            }
            OptionalDouble value = parseValue(detail.get("value"));
            if (value.isEmpty()) {
                logger.debug("Skipping non numeric metric {} with value {}", prose, detail.get("value"));
                continue;
            }
            // Checked after the value, so a skipped entry does not reserve the name against a later
            // entry that does have a number.
            String name = toMetricName(prose);
            if (name.isEmpty() || !seen.add(name)) {
                logger.debug("Skipping duplicate or unnamed metric {}", prose);
                continue;
            }
            writer.gauge(name, prose, value.getAsDouble());
        }
    }

    /**
     * Turn a prose report name into a Prometheus metric name.
     *
     * <p>Prometheus names allow only letters, digits and underscores, so everything else collapses
     * into a single underscore. "Data Rate in (GB/day)" becomes data_rate_in_gb_day.
     *
     * <p>Lowercases with {@link Locale#ROOT}: under a Turkish locale the default would turn an "I"
     * into a dotless "ı", which is past the character filter by then and would fail the scrape.
     */
    static String toMetricName(String prose) {
        String name = NOT_NAME_CHAR.matcher(prose).replaceAll("_");
        name = name.replaceAll("^_+", "").replaceAll("_+$", "");
        return name.toLowerCase(Locale.ROOT);
    }

    /**
     * Read a report value as a number, or empty if it is not one.
     *
     * <p>The reports format with the default locale, so this parses with it too rather than stripping
     * separators: under a locale where the comma is the decimal point, stripping would turn 20,73
     * into 2073. The whole string has to be consumed, otherwise "1687 of 787731" would read as 1687.
     */
    static OptionalDouble parseValue(String value) {
        if (value == null || value.isBlank()) {
            return OptionalDouble.empty();
        }
        ParsePosition position = new ParsePosition(0);
        Number parsed = NumberFormat.getInstance().parse(value.trim(), position);
        if (parsed == null || position.getIndex() != value.trim().length()) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(parsed.doubleValue());
    }
}
