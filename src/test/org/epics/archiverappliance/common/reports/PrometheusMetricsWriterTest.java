package org.epics.archiverappliance.common.reports;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Tests the archiver conventions this class layers onto the Prometheus client: the name prefix, the
 * common labels, and the exposition format version. Escaping and value rendering belong to the
 * client library and are not re-tested here.
 *
 * @author caraxlr
 */
public class PrometheusMetricsWriterTest {

    private static String render(Map<String, String> commonLabels, Consumer<PrometheusMetricsWriter> emitter)
            throws IOException {
        PrometheusMetricsWriter writer = new PrometheusMetricsWriter(commonLabels);
        emitter.accept(writer);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        writer.writeTo(buf);
        return buf.toString(StandardCharsets.UTF_8);
    }

    @Test
    public void testGaugeOutput() throws IOException {
        String out = render(Map.of("appliance", "appliance0"), w -> w.gauge("pv_count", "PVs here.", 12345));
        Assertions.assertEquals("""
                # HELP archappl_pv_count PVs here.
                # TYPE archappl_pv_count gauge
                archappl_pv_count{appliance="appliance0"} 12345.0
                """, out);
    }

    @Test
    public void testLabelsAreMerged() throws IOException {
        Map<String, String> common = new LinkedHashMap<>();
        common.put("appliance", "appliance0");
        String out = render(common, w -> w.gauge("storage_bytes", "Space.", 1024, Map.of("store", "STS")));
        Assertions.assertTrue(
                out.contains("archappl_storage_bytes{appliance=\"appliance0\",store=\"STS\"} 1024.0"),
                "Expected merged labels in " + out);
    }

    @Test
    public void testPerSampleLabelWins() throws IOException {
        String out = render(
                Map.of("appliance", "appliance0"),
                w -> w.gauge("pv_count", "PVs here.", 1, Map.of("appliance", "override")));
        Assertions.assertTrue(out.contains("appliance=\"override\""), "Expected the per sample label to win in " + out);
    }

    @Test
    public void testNoLabelsNoBraces() throws IOException {
        String out = render(null, w -> w.gauge("pv_count", "PVs here.", 1));
        Assertions.assertTrue(out.contains("\narchappl_pv_count 1.0\n"), "Expected a bare sample line in " + out);
    }

    @Test
    public void testContentType() {
        Assertions.assertEquals(
                "text/plain; version=0.0.4; charset=utf-8", new PrometheusMetricsWriter(Map.of()).getContentType());
    }

    @Test
    public void testMultipleGauges() throws IOException {
        String out = render(Map.of("appliance", "appliance0"), w -> {
            w.gauge("pv_count", "PVs here.", 3);
            w.gauge("event_rate_events_per_second", "Events per second.", 1.5);
        });
        for (String line : out.split("\n")) {
            if (line.startsWith("#")) {
                Assertions.assertTrue(
                        line.startsWith("# HELP archappl_") || line.startsWith("# TYPE archappl_"),
                        "Unexpected comment line: " + line);
            } else {
                // name[{labels}] SP value, and the value must parse as a double.
                String value = line.substring(line.lastIndexOf(' ') + 1);
                Assertions.assertDoesNotThrow(() -> Double.parseDouble(value), "Unparseable value in: " + line);
            }
        }
    }
}
