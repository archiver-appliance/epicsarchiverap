package org.epics.archiverappliance.mgmt.pva.actions;

import junit.framework.TestCase;
import org.epics.pva.data.PVAStringArray;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class NTUtilTest extends TestCase {


    public void testTestExtractStringArray() {
        String[] strings = new String[] {"hello", "there"};
        PVAStringArray stringArray = new PVAStringArray("name", strings);
        assertEquals(strings, NTUtil.extractStringArray(stringArray));

        assertArrayEquals(new String[0], NTUtil.extractStringArray(null));
    }

    public void testExtractStringList() {
        String[] strings = new String[] {"hello", "there"};
        PVAStringArray stringArray = new PVAStringArray("name", strings);
        assertEquals(Arrays.asList(strings), NTUtil.extractStringList(stringArray));
        PVAStringArray emptyArray = new PVAStringArray("name");
        assertEquals(Arrays.asList(new String[0]), NTUtil.extractStringList(emptyArray));

    }
}