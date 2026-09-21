package org.epics.archiverappliance.common.reports;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

/**
 * Tests turning the prose Details reports into Prometheus gauges.
 *
 * @author caraxlr
 */
public class DetailsMetricsConverterTest {

    /** One row of a metrics report. */
    private static Map<String, String> detail(String name, String value) {
        return Map.of("name", name, "value", value, "source", "ENGINE");
    }

    /**
     * A value as the reports would write it. They format with the default locale and the converter
     * parses with it, so a fixture hardcoded as "94,567.27" would fail wherever the comma is the
     * decimal point.
     */
    private static String reported(double value) {
        return NumberFormat.getInstance().format(value);
    }

    /** The exposition text a scrape would return for these entries. */
    private static String render(List<Map<String, String>> details) throws IOException {
        PrometheusMetricsWriter writer = new PrometheusMetricsWriter(Map.of("appliance", "appliance0"));
        DetailsMetricsConverter.addAll(writer, details);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        writer.writeTo(buf);
        return buf.toString(StandardCharsets.UTF_8);
    }

    @ParameterizedTest
    @CsvSource({
        "Total PV count,                     total_pv_count",
        "Data Rate in (GB/day),              data_rate_in_gb_day",
        "Write cycle scheduling load (%),    write_cycle_scheduling_load",
        "Benchmark - writing at (MB/sec),    benchmark_writing_at_mb_sec",
        "Avg time spent by markForDeletion() in ETL (s/run), avg_time_spent_by_markfordeletion_in_etl_s_run",
        "(Total PV count),                   total_pv_count",
        "already_clean,                      already_clean"
    })
    public void testToMetricName(String prose, String expected) {
        Assertions.assertEquals(expected, DetailsMetricsConverter.toMetricName(prose));
    }

    @Test
    public void testNumbersParse() {
        Assertions.assertEquals(
                12345.0, DetailsMetricsConverter.parseValue("12345").getAsDouble());
        Assertions.assertEquals(
                155.24, DetailsMetricsConverter.parseValue(reported(155.24)).getAsDouble());
        // The reports format with grouping separators, so a bare Double.parseDouble would reject this.
        Assertions.assertEquals(
                94567.27, DetailsMetricsConverter.parseValue(reported(94567.27)).getAsDouble());
    }

    @ParameterizedTest
    @ValueSource(strings = {"N/A", "Never", "In Progress", "Working", "Sep/10/2026 13:50:03 CDT", ""})
    public void testNonNumbersAreSkipped(String value) {
        Assertions.assertTrue(
                DetailsMetricsConverter.parseValue(value).isEmpty(), "Expected no value parsed from " + value);
    }

    @Test
    public void testPartiallyNumericValueIsRejected() {
        // NumberFormat stops at the space and returns 1687, which would publish a wrong number.
        Assertions.assertTrue(
                DetailsMetricsConverter.parseValue("1687 of 787731").isEmpty(),
                "Expected the whole string to have to parse");
    }

    @Test
    public void testNullValueIsSkipped() {
        Assertions.assertTrue(DetailsMetricsConverter.parseValue(null).isEmpty());
    }

    @Test
    public void testDuplicateNamesAreSkippedRatherThanFailing() throws IOException {
        // The ETL report repeats a name once per store, and two samples with identical labels would
        // otherwise make the client reject the whole payload.
        String out = render(List.of(
                detail("Average percentage of time spent in ETL", reported(0.19)),
                detail("Average percentage of time spent in ETL", reported(36.34))));
        Assertions.assertEquals(
                1,
                out.lines()
                        .filter(l -> l.startsWith("archappl_average_percentage"))
                        .count(),
                "Expected only the first of the colliding entries in " + out);
        Assertions.assertTrue(out.contains("} 0.19"), "Expected the first entry kept in " + out);
    }

    @Test
    public void testNonNumericEntriesDoNotReachTheOutput() throws IOException {
        String out = render(List.of(detail("Total PV count", reported(48213)), detail("Startup", "In Progress")));
        Assertions.assertTrue(out.contains("archappl_total_pv_count{appliance=\"appliance0\"} 48213.0"));
        Assertions.assertFalse(out.contains("startup"), "Expected the non numeric entry skipped in " + out);
    }

    @Test
    public void testProseBecomesTheHelpText() throws IOException {
        // The prose is the only description Prometheus gets, so it has to survive naming.
        String out = render(List.of(detail("Data Rate (in bytes/sec)", reported(1024))));
        Assertions.assertTrue(
                out.contains("# HELP archappl_data_rate_in_bytes_sec Data Rate (in bytes/sec)"),
                "Expected the prose as help text in " + out);
    }

    @Test
    public void testEntryWithoutANameIsSkipped() throws IOException {
        Assertions.assertEquals("", render(List.of(Map.of("value", "42", "source", "ENGINE"))));
        Assertions.assertEquals("", render(List.of(detail("   ", "42"))));
    }

    @Test
    public void testProseThatLeavesNoNameIsSkipped() throws IOException {
        // All punctuation, so nothing is left to name the metric once the illegal characters go.
        Assertions.assertEquals("", render(List.of(detail("(%)", "42"))));
    }
}
