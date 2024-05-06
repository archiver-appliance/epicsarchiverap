package org.epics.archiverappliance.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

/**
 * Small unit test to make sure the regex that checks for valid pv names does something reasonable
 * @author mshankar
 *
 */
public class PVNameRegexTest {

    private static final List<String> validPVNames = List.of(
            "archappl:arch:sine",
            "archappl:arch:sine1",
            "archappl:arch:sine1",
            "archappl:arch:sine100",
            "archappl_arch_sine",
            "archappl-arch-sine",
            "archappl+arch+sine",
            "archappl[arch]sine",
            "archappl<arch>sine",
            "archappl:arch;sine",
            "archappl:ar/h:sine",
            "P#6:000357",
            "LN-AM{RadMon:1}Lvl:Raw-I");

    private static final String valFieldName = ".VAL";
    private static final List<String> validFieldNames = List.of(".HIHI", ".LO", "");
    private static final List<String> validFieldModifiers = List.of(
            ".{'dbnd':{'abs':1}}",
            ".{flv('H-GX')}",
            ".{'dbnd':{'abs':1.5}}",
            ".{\"dbnd\":{'a':'b',\"s\",'b'}}",
            ".{'dbnd':{'hi':1.5},'a':{'b':'c'}}",
            ".[3:5]",
            "");

    private static Stream<Arguments> provideValidChannelNames() {
        return validPVNames.stream()
                .flatMap(pvName -> validFieldNames.stream().map(fieldName -> pvName + fieldName))
                .flatMap(pvAndFieldName -> validFieldModifiers.stream()
                        .map(fieldModifier -> Arguments.of(pvAndFieldName + fieldModifier)));
    }
    /**
     * Tests list of pv names valid for the archiver. Larger than the list documented in <a href="https://docs.epics-controls.org/en/latest/appdevguide/databaseDefinition.html#definitions-8">App dev Guide</a>
     * due to some labs using other characters. {@link PVNames#isValidChannelName}.
     */
    @ParameterizedTest
    @MethodSource("provideValidChannelNames")
    public void testValidChannelNames(String pvName) {
        Assertions.assertTrue(PVNames.isValidChannelName(pvName), "Valid pvName is deemed invalid " + pvName);
        Assertions.assertEquals(pvName, PVNames.normalizeChannelName(pvName));
    }

    private static Stream<Arguments> providePVNames() {
        return validPVNames.stream().flatMap(pvName -> validFieldModifiers.stream()
                .map(fieldModifier -> Arguments.of(pvName, fieldModifier)));
    }

    @ParameterizedTest
    @MethodSource("providePVNames")
    public void testNormalizePVName(String pvName, String fieldModifier) {
        String channelName = pvName + valFieldName + fieldModifier;
        Assertions.assertEquals(pvName + fieldModifier, PVNames.normalizeChannelName(channelName));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "\"archappl:arch:sine",
                "archappl:arch:sine\"",
                "archappl:ar!h:sine",
                "archappl:ar$h:sine",
                "archappl:ar%h:sine",
                "archappl:ar*h:sine",
                "archappl:ar\\h:sine",
                "archappl:ar|h:sine",
                "archappl:ar?h:sine",
                "archappl:ar&h:sine",
                "archappl:ar@ch:sine",
                "archappl:(arch):sine",
                "archappl:arch:sine=1.0",
                "archappl:sine.HI'",
                "archappl:sine.HI.BOO",
                "archappl:sine.HI.",
                "archappl:sine.",
                "archappl:sine.{",
                "archappl:sine.}",
                "archappl:sine.5",
                ""
            })
    public void testInvalidPVNames(String pvName) {
        Assertions.assertFalse(PVNames.isValidChannelName(pvName), "Invalid pvName is deemed valid " + pvName);

        Assertions.assertFalse(PVNames.isValidChannelName(null), "null is deemed invalid");
    }
}
