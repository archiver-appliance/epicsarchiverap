package org.epics.archiverappliance.utils.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is a replacement for NIO Paths that caters to our syntax rules.
 * Normal file system paths are all similar to linux file names.
 * The compressed use the jar:file:syntax.
 * For example, <code>jar:file:///ziptest/alltext.zip!/SomeTextFile.txt</code>
 *
 * @author luofeng
 * @author mshankar
 *
 */
public class ArchPaths implements Closeable {
    public static final String ZIP_PREFIX = "jar:file://";
    private static final Logger logger = LogManager.getLogger(ArchPaths.class.getName());
    private final ConcurrentHashMap<URI, FileSystem> fileSystemList = new ConcurrentHashMap<URI, FileSystem>();

    /**
     * Returns a seekable byte channel.
     * In case of file systems, this is the raw SeekableByteChannel as returned by the provider.
     * In case of zip files, we wrap the InputStream using WrappedSeekableByteChannel (which is a read only byte channel for now).
     *
     * @param path    Path
     * @param options OpenOption
     * @return a new seekabel byte channel
     * @throws IOException &emsp;
     */
    public static SeekableByteChannel newByteChannel(Path path, OpenOption... options) throws IOException {
        String scheme = path.toUri().getScheme();
        if (scheme != null && PVPath.SCHEME_TO_PACKFILE_EXTENSION.containsKey(scheme)) {
            return new WrappedSeekableByteChannel(path);
        } else {
            return Files.newByteChannel(path, options);
        }
    }

    public Path get(PVPath pvPath) throws IOException {
        return this.get(pvPath, false);
    }

    public Path get(PVPath pvPath, boolean createParentFolder) throws IOException {
        if (pvPath.shouldWeCreateParentFolder() && createParentFolder) {
            Path parentPath = Paths.get(pvPath.getParentPathForCreation());
            if (logger.isDebugEnabled()) logger.debug("Creating parent folder " + parentPath);
            if (Files.notExists(parentPath)) {
                Files.createDirectories(parentPath);
            }
        }

        if (pvPath.isPlainFile()) {
            logger.debug("Returning plain file path " + pvPath.getFullPath());
            return Paths.get(pvPath.getFullPath());
        }

        if (pvPath.isRootFolderOnly()) {
            return Paths.get(pvPath.getFullPath());
        }

        FileSystem fs = null;
        URI rootURI = pvPath.toRootURI();
        if (this.fileSystemList.containsKey(rootURI)) {
            fs = this.fileSystemList.get(rootURI);
        } else {
            // Should we do a getFileSystem here and it that thread safe?
            fs = FileSystems.newFileSystem(rootURI, Map.of("create", createParentFolder ? "true" : "false"), this.getClass().getClassLoader());
            this.fileSystemList.put(rootURI, fs);
        }

        return fs.getPath(pvPath.getContainedPath());
    }

    public record GlobSearchParams(Path searchFolder, String globPattern) {}

    /*
     * For listing the files in a folder, we do a GLOB search in a folder.
     * The search folder depends on whether this represents a pack file.
     */
    public GlobSearchParams getGlobSearchParams(PVPath pvPath, String extension) throws IOException {
        if (pvPath.getChunkKey() == null) {
            throw new IllegalStateException("chunkKey is required to determine glob search params");
        }
        String noterm = pvPath.getChunkKey().substring(0, pvPath.getChunkKey().length() - 1);
        String terminator = pvPath.getChunkKey().substring(pvPath.getChunkKey().length() - 1);
        Path notermPath = Path.of(noterm);
        if (!pvPath.isPackFile()) {
            Path par = notermPath.getParent();
            return new GlobSearchParams(
                    Path.of(pvPath.getRootFolder(), par != null ? par.toString() : ""),
                    notermPath.getFileName().toString() + terminator + "*" + extension);
        }
        Path cPath = this.get(new PVPath(pvPath.getRootFolder(), pvPath.getChunkKey(), null, null));
        return new GlobSearchParams(cPath, notermPath.getFileName().toString() + terminator + "*" + extension);
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<URI, FileSystem> entry : fileSystemList.entrySet()) {
            if (logger.isDebugEnabled())
                logger.debug("Closing file system for " + entry.getKey().toString());
            entry.getValue().close();
        }
    }
}
