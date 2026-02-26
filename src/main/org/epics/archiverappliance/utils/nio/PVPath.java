package org.epics.archiverappliance.utils.nio;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;

/*
 * Encapsulates the various distinct components of a Parquet/PB file in EAA.
 */

public class PVPath {
    private final String rootFolder;
    /* Per convertPVNameToKey, the chunkKey here includes the terminator character */
    private final String chunkKey;
    private final char terminator;
    /* The portion of the path that corresponds to the partition granularity */
    private final String partitionName;
    /* The file extension, including the dot (e.g., ".parquet") */
    private final String extension;
    /* Derived attributes */
    /* Does this start with a *scheme*://. Note paths that start with file:// are also considered NIO2 paths */
    private final boolean isPackFile;
    /* The NIO2 scheme for this path */
    private final String scheme;
    /* This path specifies the root folder only. Primarily used to indicate that we do not expect a chunk key etc for this path */
    private final boolean rootFolderOnly;

    public static final Map<String, String> SCHEME_TO_PACKFILE_EXTENSION = Map.of(
            "zip", ".zip",
            "jar", ".zip",
            "tar", ".tar",
            "gztar", ".tar");

    public PVPath(String rootFolder, String chunkKey, String partitionName, String extension, boolean rootFolderOnly) {
        this.rootFolder = rootFolder;
        this.chunkKey = chunkKey;
        this.terminator = chunkKey != null && !chunkKey.isEmpty() ? chunkKey.charAt(chunkKey.length() - 1) : 0;
        this.partitionName = partitionName;
        this.extension = extension;
        this.rootFolderOnly = rootFolderOnly;
        if (rootFolder.contains(":/")) {
            this.scheme = rootFolder.substring(0, rootFolder.indexOf(":/")).substring(0, rootFolder.indexOf(":"));
            this.isPackFile = SCHEME_TO_PACKFILE_EXTENSION.containsKey(this.scheme);
        } else {
            this.scheme = null; // Should this be "file"?
            this.isPackFile = false;
        }
    }

    public static PVPath fromRootFolder(String rootFolder) {
        return new PVPath(rootFolder, null, null, null, true);
    }

    public PVPath(String rootFolder, String chunkKey) {
        this(rootFolder, chunkKey, null, null, false);
    }

    public PVPath(String rootFolder, String chunkKey, String partitionName, String extension) {
        this(rootFolder, chunkKey, partitionName, extension, false);
    }

    public String getRootFolder() {
        return rootFolder;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getExtension() {
        return extension;
    }

    public boolean isPackFile() {
        return isPackFile;
    }

    public boolean isNIO2Path() {
        return this.scheme != null;
    }

    public boolean isPlainFile() {
        return !this.isPackFile && (this.scheme == null || this.scheme.equals("file"));
    }

    public boolean shouldWeCreateParentFolder() {
        return (this.scheme == null
                || this.scheme.equals("file")
                || (this.scheme != null && SCHEME_TO_PACKFILE_EXTENSION.containsKey(this.scheme)));
    }

    public boolean isRootFolderOnly() {
        return rootFolderOnly;
    }

    public String getFullPath() {
        StringBuilder fullPath = new StringBuilder(rootFolder);
        if (chunkKey != null) {
            if (!rootFolder.endsWith("/")) {
                fullPath.append("/");
            }
            if (this.isPackFile) {
                fullPath.append(chunkKey.substring(0, chunkKey.length() - 1));
                fullPath.append(SCHEME_TO_PACKFILE_EXTENSION.get(this.scheme));
            } else {
                fullPath.append(chunkKey);
            }
        }
        if (partitionName != null) {
            if (this.isPackFile) {
                fullPath.append("!/");
                if (chunkKey != null) {
                    Path ckPath = Path.of(chunkKey);
                    Path finalComponent = ckPath.getFileName();
                    fullPath.append(finalComponent);
                }
            }
            fullPath.append(partitionName);
        }
        if (extension != null) {
            fullPath.append(extension);
        }
        return fullPath.toString();
    }

    public URI toURI() {
        return URI.create(this.getFullPath());
    }

    /*
     * This can be used as the key in any cache of FileSystem objects.
     */
    public URI toRootURI() {
        if (this.isPlainFile()) {
            return URI.create(this.rootFolder);
        }
        if (this.isNIO2Path() && !this.isPackFile) {
            return URI.create(this.rootFolder);
        }

        StringBuilder rootURI = new StringBuilder();
        rootURI.append(rootFolder.substring(0, rootFolder.indexOf("://") + 3));
        rootURI.append(this.getContainerPath());
        return URI.create(rootURI.toString());
    }

    public String getParentPathForCreation() {
        StringBuilder parentPath = new StringBuilder();
        if (this.isPackFile) {
            parentPath.append(rootFolder.substring(rootFolder.indexOf("://") + 3));
        } else {
            parentPath.append(rootFolder);
        }
        if (chunkKey != null) {
            if (chunkKey.contains("/")) {
                if (!rootFolder.endsWith("/")) {
                    parentPath.append("/");
                }
                parentPath.append(chunkKey, 0, chunkKey.lastIndexOf("/"));
            }
        }
        return parentPath.toString();
    }

    /*
     * Return the name of a NIO2 container file.
     * For example, for jar:file:/scratch/zipfiles/flot-0.7.zip!/FAQ.txt, we'd return /scratch/zipfiles/flot-0.7.zip
     */
    public String getContainerPath() {
        if (!this.isPackFile) {
            throw new UnsupportedOperationException("getContainerPath is only supported for NIO2 paths");
        }
        if (chunkKey == null) {
            throw new IllegalStateException("chunkKey is required to determine container path");
        }
        StringBuilder containerPath = new StringBuilder(rootFolder.substring(rootFolder.indexOf("://") + 3));
        if (!rootFolder.endsWith("/")) {
            containerPath.append("/");
        }
        containerPath.append(chunkKey.substring(0, chunkKey.length() - 1));
        containerPath.append(SCHEME_TO_PACKFILE_EXTENSION.get(this.scheme));
        return containerPath.toString();
    }

    /*
     * Return the name of a the file within the NIO2 container.
     * For example, for jar:file:/scratch/zipfiles/flot-0.7.zip!/FAQ.txt, we'd return /FAQ.txt
     * For non-packed files, this is the part of the path after the root folder
     */
    public String getContainedPath() {
        if (this.isRootFolderOnly()) {
            return "/";
        }

        if (!this.isPackFile()) {
            StringBuilder containedPath = new StringBuilder();
            if (chunkKey != null) {
                if (!chunkKey.startsWith("/")) {
                    containedPath.append("/");
                }
                containedPath.append(chunkKey);
            }
            if (partitionName != null) {
                containedPath.append(partitionName);
            }
            if (extension != null) {
                containedPath.append(extension);
            }
            return containedPath.toString();
        }

        if (this.partitionName == null) {
            return "/";
        }

        StringBuilder containedPath = new StringBuilder();
        if (!partitionName.startsWith("/")) {
            containedPath.append("/");
        }
        if (chunkKey != null) {
            Path ckPath = Path.of(chunkKey);
            Path finalComponent = ckPath.getFileName();
            containedPath.append(finalComponent);
        }
        containedPath.append(partitionName);
        if (extension != null) {
            containedPath.append(extension);
        }
        return containedPath.toString();
    }

    public static PVPath fromPath(String pathStr, String chunkKey) {
        boolean isNIO2Path = pathStr.contains(":/");
        String chunkKeyWithoutTerminator = isNIO2Path ? chunkKey.substring(0, chunkKey.length() - 1) : chunkKey;
        String rootFolder = pathStr.substring(0, pathStr.indexOf(chunkKeyWithoutTerminator));
        String extension = null;
        String partitionName = null;
        if (isNIO2Path) {
            if (pathStr.contains("!/")) {
                String restOfPath = isNIO2Path
                        ? pathStr.substring(pathStr.lastIndexOf("!/") + 2)
                        : pathStr.substring(pathStr.indexOf(chunkKey) + chunkKey.length());
                extension = restOfPath.contains(".") ? restOfPath.substring(restOfPath.lastIndexOf(".")) : null;
                partitionName = extension != null ? restOfPath.substring(0, restOfPath.indexOf(extension)) : restOfPath;
            }
        } else {
            String restOfPath = pathStr.substring(pathStr.indexOf(chunkKey) + chunkKey.length());
            extension = restOfPath.contains(".") ? restOfPath.substring(restOfPath.lastIndexOf(".")) : null;
            partitionName = extension != null ? restOfPath.substring(0, restOfPath.indexOf(extension)) : restOfPath;
        }

        if (rootFolder.endsWith("/")) {
            rootFolder = rootFolder.substring(0, rootFolder.length() - 1);
        }
        if (partitionName != null && partitionName.isBlank()) {
            partitionName = null;
        }
        return new PVPath(rootFolder, chunkKey, partitionName, extension);
    }

    public PVPath withNewExtension(String newExtension) {
        return new PVPath(this.rootFolder, this.chunkKey, this.partitionName, newExtension);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PVPath other = (PVPath) obj;
        return rootFolder.equals(other.rootFolder)
                && ((chunkKey == null && other.chunkKey == null)
                        || (chunkKey != null && chunkKey.equals(other.chunkKey)))
                && ((partitionName == null && other.partitionName == null)
                        || (partitionName != null && partitionName.equals(other.partitionName)))
                && ((extension == null && other.extension == null)
                        || (extension != null && extension.equals(other.extension)));
    }

    @Override
    public String toString() {
        return this.getFullPath();
    }
}
