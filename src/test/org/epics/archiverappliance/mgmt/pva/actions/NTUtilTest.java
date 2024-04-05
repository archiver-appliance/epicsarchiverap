package org.epics.archiverappliance.mgmt.pva.actions;

import org.epics.pva.data.PVAStringArray;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class NTUtilTest {

    @Test
    public void testTestExtractStringArray() {
        String[] strings = new String[] {"hello", "there"};
        PVAStringArray stringArray = new PVAStringArray("name", strings);
        assertEquals(strings, NTUtil.extractStringArray(stringArray));

        assertArrayEquals(new String[0], NTUtil.extractStringArray(null));
    }

    @Test
    public void testExtractStringList() {
        String[] strings = new String[] {"hello", "there"};
        PVAStringArray stringArray = new PVAStringArray("name", strings);
        assertEquals(Arrays.asList(strings), NTUtil.extractStringList(stringArray));
        PVAStringArray emptyArray = new PVAStringArray("name");
        assertEquals(Arrays.asList(new String[0]), NTUtil.extractStringList(emptyArray));
    }
}
