/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.utils;

import static edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin.pbFileExtension;

import edu.stanford.slac.archiverappliance.plain.PathNameUtility;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.utils.nio.ArchPaths;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Utility to check what files are ready for ETL for a given PV, folder and partition granularity.
 * @author mshankar
 *
 */
public class GetETLReadyFiles {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                    "Usage: java edu.stanford.slac.archiverappliance.plain.utils.GetETLReadyFiles <PVName> <FolderName> <Granularity>");
            return;
        }
        ConfigService configService = new ConfigServiceForTests(-1);
        String pvName = args[0];
        File folder = new File(args[1]);
        PartitionGranularity granularity = PartitionGranularity.valueOf(args[2]);
        if (granularity == null) {
            throw new Exception("Unable to determine granularity for " + args[2]);
        }

        Instant now = TimeUtils.now();
        Path[] paths = PathNameUtility.getPathsBeforeCurrentPartition(
                new ArchPaths(),
                folder.getAbsolutePath(),
                pvName,
                now,
                pbFileExtension,
                granularity,
                PathResolver.BASE_PATH_RESOLVER,
                configService.getPVNameToKeyConverter());
        if (paths == null || paths.length == 0) {
            System.out.println("No files for pv " + pvName + " before current partition using time "
                    + TimeUtils.convertToHumanReadableString(now));
        }
    }
}
