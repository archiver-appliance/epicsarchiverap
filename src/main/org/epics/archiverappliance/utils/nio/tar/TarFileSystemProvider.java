package org.epics.archiverappliance.utils.nio.tar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Given a path of the form gztar:///arch/XLTS/mshankar/arch/gztartest:2016.pb, we map it to the tar file /arch/XLTS/mshankar/arch/gztartest.tar
 * and the entry 2016.pb within the tar file.
 * Each gztar file is a new file system.
 */

public class TarFileSystemProvider extends FileSystemProvider {
    static final Logger logger = LogManager.getLogger(TarFileSystemProvider.class.getName());
    private final Map<Path, TarFileSystem> filesystems = new ConcurrentHashMap<Path, TarFileSystem>();

    @Override
    public String getScheme() {
        return URIUtils.GZTAR_SCHEME;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        logger.debug("New file system for " + uri.toString());
        Path pathToTarFile = URIUtils.getPathToTarFile(uri);
        logger.debug("Creating new file system for {}", pathToTarFile.toString());

        TarFileSystem gztarfs = new TarFileSystem(this, pathToTarFile, env);
        filesystems.put(pathToTarFile, gztarfs);
        logger.debug("Done creating new file system for {}", pathToTarFile.toString());
        return gztarfs;
    }

    @Override
    public FileSystem newFileSystem(Path path, Map<String, ?> env) throws IOException {
        logger.debug("New file system for path {}", path.toString());
        return this.newFileSystem(path.toUri(), env);
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        logger.debug("Existing file system for path {}", uri.toString());
        Path pathToTarFile = URIUtils.getPathToTarFile(uri);
        return filesystems.get(pathToTarFile);
    }

    @Override
    public Path getPath(URI uri) {
        logger.debug("Convert to path {}", uri.toString());
        String spec = uri.getSchemeSpecificPart();
        int sep = spec.indexOf("!/");
        if (sep == -1)
            throw new IllegalArgumentException(
                    "URI: " + uri + " does not contain path info ex. gztar:file:/c:/foo.zip!/BAR");
        return getFileSystem(uri).getPath(spec.substring(sep + 1));
    }

    /*
     * Get a byte channel into the TarEntry.
     * Only a few options are supported.
     * READ - We extract and gunzip the contents into temp folder and return a readable channel into the extracted file.
     * APPEND - If the the tar entry is present, we extract and gunzip the contents into temp folder and return a writable channel into the extracted file; updating the tar entry on close.
     * TRUNCATE_EXISTING - We create a new file in the temp folder return a writable channel into the extracted file; updating the tar entry on close.
     * CREATE - Only in addition to APPEND and TRUNCATE_EXISTING
     */
    @Override
    public SeekableByteChannel newByteChannel(
            Path srcpath, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        if (!(srcpath instanceof TarPath)) {
            throw new IOException("Path " + srcpath.toString() + " is not a GZPath"); // Why are we even here?
        }
        TarPath path = (TarPath) srcpath;
        TarEntry tarEntry = path.getTarEntry();
        TarFileSystem gfs = (TarFileSystem) path.getFileSystem();
        // Get the latest tar entry from the catalog in case something has written to it behind our backs.
        tarEntry = gfs.lookupTarEntry(tarEntry.entryName(), tarEntry);
        logger.debug(
                "Opening byte channel for path {} with options {}, resolved tar entry is {}",
                path.toUri().toString(),
                options.toString(),
                tarEntry);
        boolean openForRead = options.isEmpty() || options.contains(StandardOpenOption.READ);
        boolean openForWrite = options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.CREATE)
                || options.contains(StandardOpenOption.CREATE_NEW) || options.contains(StandardOpenOption.APPEND)
                || options.contains(StandardOpenOption.TRUNCATE_EXISTING);
        if (openForWrite) {
            openForRead = false;
        }

        if (openForRead) {
            if (!tarEntry.isInsideTar()) {
                throw new IOException("Path " + path.toString() + " does not have an entry in the tar file");
            }
            return gfs.getTarFile().getReadOnlyByteChannel(tarEntry);
        }

        if (openForWrite) {
            if(tarEntry.isInsideTar()) {
                if(options.contains(StandardOpenOption.CREATE_NEW)) {
                    throw new FileAlreadyExistsException("Path " + srcpath.toString() + " already exists in the tar file");
                }
            }
            if(!tarEntry.isInsideTar()) {
                // We do not have a entry in the tar file.
                boolean okToCreateFile = options.contains(StandardOpenOption.CREATE) || options.contains(StandardOpenOption.CREATE_NEW);
                if (!okToCreateFile) {
                    throw new IOException("Path " + srcpath.toString() + " does not have an entry in the tar file");
                }
            }
            File tmpFile = File.createTempFile("__eaa", ".pb");
            if (options.contains(StandardOpenOption.APPEND)) {
                logger.debug("Extracting existing content for APPEND for {}", tarEntry.entryName());
                try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
                    try (FileChannel tmpChannel = fos.getChannel()) {
                        gfs.getTarFile().extract(tarEntry, tmpChannel);
                    }
                }
            }
            @SuppressWarnings("resource")
            FileOutputStream fos = new FileOutputStream(tmpFile, options.contains(StandardOpenOption.APPEND));
            FileChannel channel = fos.getChannel();
            return new TarSeekableByteChannel(fos, channel, tarEntry, gfs, tmpFile, tmpFile.toPath());
        }
        throw new IOException("Unsupported channel open options " + options.toString());
    }

    private class GZTarDirectoryStream implements DirectoryStream<Path> {
        private TarFileSystem gfs;
        private DirectoryStream.Filter<? super Path> filter;

        public GZTarDirectoryStream(TarFileSystem gfs, DirectoryStream.Filter<? super Path> filter) {
            this.gfs = gfs;
            this.filter = filter;
        }

        @Override
        public void close() throws IOException {
            // Not much to do here...
        }

        @Override
        public Iterator<Path> iterator() {
            List<Path> ret = new LinkedList<Path>();
            Map<String, TarEntry> catalog = this.gfs.getTarFileCatalog();
            for (TarEntry tarEntry : catalog.values()) {
                TarPath path = new TarPath(gfs, tarEntry);
                try {
                    if (this.filter.accept(path)) {
                        logger.debug("Adding {}", tarEntry.entryName());
                        ret.add(path);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            Collections.sort(ret);
            return ret.iterator();
        }
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
            throws IOException {
        if (!(dir instanceof TarPath)) {
            throw new IOException("Path " + dir.toString() + " is not a GZPath"); // Why are we even here?
        }
        TarPath path = (TarPath) dir;
        TarFileSystem gfs = (TarFileSystem) path.getFileSystem();
        return new GZTarDirectoryStream(gfs, filter);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        throw new IOException("Unsupported operation");
    }

    @Override
    public void delete(Path path) throws IOException {
        if (!(path instanceof TarPath)) {
            throw new IOException("Path " + path.toString() + " is not a GZPath"); // Why are we even here?
        }
        TarPath gzpath = (TarPath) path;

        if (gzpath.getTarEntry() == null) {
            throw new NoSuchFileException(
                    "Path " + path.toString() + " was not found in the tar file"); // Why are we even here?
        }
        TarFileSystem gfs = (TarFileSystem) path.getFileSystem();
        TarEntry tarEntry = gfs.lookupTarEntry(gzpath.getTarEntry().entryName());
        if (tarEntry == null || !tarEntry.isInsideTar()) {
            throw new NoSuchFileException(
                    "Path " + path.toString() + " was not found in the tar file"); // Why are we even here?
        }
        gfs.tarFile.markFileAsDeleted(tarEntry);
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        throw new IOException("Unsupported operation");
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        if (!(source instanceof TarPath) || !(target instanceof TarPath)) {
            throw new IOException("Path " + source.toString() + " or " + target.toString()
                    + " is not a GZPath"); // Why are we even here?
        }
        TarPath srGzPath = (TarPath) source;
        TarPath tgGzPath = (TarPath) target;
        TarFileSystem srGfs = srGzPath.fileSystem;
        TarFileSystem tgGfs = tgGzPath.fileSystem;
        logger.debug(
                "Moving path {} to {} from tar {} to tar {}",
                srGzPath,
                tgGzPath,
                srGfs.tarFile.getTarFileName(),
                tgGfs.tarFile.getTarFileName());
        if (!srGfs.tarFile.getTarFileName().equals(tgGfs.tarFile.getTarFileName())) {
            throw new IOException("Cannot move between tar files " + srGfs.tarFile.getTarFileName() + " and "
                    + tgGfs.tarFile.getTarFileName());
        }

        if (tgGfs.lookupTarEntry(tgGzPath.tarEntry.entryName()) != null) {
            if (!(Set.of(options).contains(StandardCopyOption.ATOMIC_MOVE)
                    || Set.of(options).contains(StandardCopyOption.REPLACE_EXISTING))) {
                throw new FileAlreadyExistsException("Path " + target.toString() + " already exists in "
                        + tgGzPath.fileSystem.tarFile.getTarFileName());
            }
            tgGfs.getTarFile().markFileAsDeleted(tgGzPath.tarEntry);
            tgGfs.reloadCatalog();
        }
        srGfs.tarFile.renameEntry(srGzPath.tarEntry, tgGzPath.tarEntry.entryName());
        srGfs.reloadCatalog();
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        throw new IOException("Unsupported operation");
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        if (!(path instanceof TarPath)) {
            throw new IOException("Path " + path.toString() + " is not a GZPath"); // Why are we even here?
        }
        TarPath gzpath = (TarPath) path;
        return Files.getFileStore(gzpath.fileSystem.tarPath.getParent());
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        logger.debug("Checking access for path {}", path.toString());
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
            throws IOException {
        if (!(path instanceof TarPath)) {
            throw new IOException("Path " + path.toString() + " is not a GZPath"); // Why are we even here?
        }
        TarPath gzpath = (TarPath) path;
        return gzpath.getBasicFileAttributes(type, options);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new IOException("Unsupported operation");
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new IOException("Unsupported operation");
    }

    public void closeFileSystem(String tarFileName, TarFileSystem fs) {
        this.filesystems.remove(Paths.get(tarFileName));
    }
}
