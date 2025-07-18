package org.epics.archiverappliance.mgmt;

import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.utils.ui.JSONDecoder;
import org.epics.archiverappliance.utils.ui.JSONEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests JSON Encoder and Decoder to make sure that all required methods have been set up.
 * @author mshankar
 *
 */
public class JSONEncodeDecodeMethodCheckTest {

    @BeforeEach
    public void setUp() throws Exception {}

    @AfterEach
    public void tearDown() throws Exception {}

    @Test
    public void testJSONEncodeDecodeMethodCheck() throws Exception {
        // Add one line for each class that we expect to be encoded/decoded into JSON
        checkEncoderDecoder(PVTypeInfo.class);
        checkEncoderDecoder(UserSpecifiedSamplingParams.class);
        checkEncoderDecoder(ApplianceAggregateInfo.class);
        checkEncoderDecoder(MetaInfo.class);
    }

    private void checkEncoderDecoder(Class<? extends Object> clazz) throws Exception {
        JSONEncoder.getEncoder(clazz);
        JSONDecoder.getDecoder(clazz);
    }
}
