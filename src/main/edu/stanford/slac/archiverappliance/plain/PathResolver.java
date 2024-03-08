package edu.stanford.slac.archiverappliance.plain;

import org.epics.archiverappliance.utils.nio.ArchPaths;

import java.io.IOException;
import java.nio.file.Path;

public interface PathResolver {
    PathResolver BASE_PATH_RESOLVER = (paths, createParentFolder, rootFolder, pvComponent, pvKey) ->
            paths.get(createParentFolder, rootFolder, pvComponent);

    Path get(ArchPaths paths, boolean createParentFolder, String rootFolder, String pvPathComponent, String pvKey)
            throws IOException;
}
