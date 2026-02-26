package edu.stanford.slac.archiverappliance.plain;

import org.epics.archiverappliance.utils.nio.ArchPaths;
import org.epics.archiverappliance.utils.nio.PVPath;

import java.io.IOException;
import java.nio.file.Path;

public interface PathResolver {
    PathResolver BASE_PATH_RESOLVER = (paths, createParentFolder, pvPath) -> paths.get(pvPath, createParentFolder);

    Path get(ArchPaths paths, boolean createParentFolder, PVPath pvPath) throws IOException;
}
