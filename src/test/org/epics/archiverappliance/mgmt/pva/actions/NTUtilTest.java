package org.epics.archiverappliance.mgmt.pva.actions;

import org.epics.pva.data.PVAStringArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class NTUtilTest {

    @Test
    public void testTestExtractStringArray() {
        String[] strings = new String[] {"hello", "there"};
        PVAStringArray stringArray = new PVAStringArray("name", strings);
        Assertions.assertEquals(strings, NTUtil.extractStringArray(stringArray));

        Assertions.assertArrayEquals(new String[0], NTUtil.extractStringArray(null));
    }

    @Test
    public void testExtractStringList() {
        String[] strings = new String[] {"hello", "there"};
        PVAStringArray stringArray = new PVAStringArray("name", strings);
        Assertions.assertEquals(Arrays.asList(strings), NTUtil.extractStringList(stringArray));
        PVAStringArray emptyArray = new PVAStringArray("name");
        Assertions.assertEquals(Arrays.asList(new String[0]), NTUtil.extractStringList(emptyArray));
    }
}
