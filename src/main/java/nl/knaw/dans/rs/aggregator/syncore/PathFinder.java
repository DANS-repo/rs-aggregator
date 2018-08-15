package nl.knaw.dans.rs.aggregator.syncore;

import nl.knaw.dans.rs.aggregator.util.NormURI;
import nl.knaw.dans.rs.aggregator.util.RsProperties;
import nl.knaw.dans.rs.aggregator.util.ZonedDateTimeUtil;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class capable of finding its way in file system directories for resources, metadata and synchronisation properties
 * for a given base directory and URI.
 */
public class PathFinder {

    public static final String DIR_METADATA = "__MOR__";
    public static final String DIR_RESOURCES = "__SOR__";
    public static final String DIR_SYNC_PROPS = "__SYNC_PROPS__";

    private static Logger logger = LoggerFactory.getLogger(PathFinder.class);

    private final ZonedDateTime syncStart;

    private final String host;
    private final int port;
    private final String path;

    private final URI capabilityListUri;

    private final File baseDirectory;
    private final File setDirectory;
    private final File metadataDirectory;
    private final File resourceDirectory;
    private final File syncPropDirectory;
    private final File syncPropXmlFile;
    private final File prevSyncPropXmlFile;
    private final File capabilityListFile;

    private File descriptionFile;

    public PathFinder(@Nonnull String baseDirectory, @Nonnull URI capabilityListUri) {
        this.capabilityListUri = capabilityListUri;
        syncStart = ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC);
        File baseDir = new File(baseDirectory);
        this.baseDirectory = baseDir.getAbsoluteFile();

        URI uri2 = NormURI.normalize(capabilityListUri).orElse(null);
        host = uri2.getHost();
        port = uri2.getPort();
        File filePath = new File(uri2.getPath());
        path = filePath.getParent();
        String fileName = filePath.getName();

        StringBuilder sb = new StringBuilder(this.baseDirectory.getAbsolutePath())
          .append(File.separator)
          .append(host);
        if (port > -1) {
            sb.append(File.separator).append(port);
        }
        sb.append(path);
        setDirectory = new File(sb.toString());
        metadataDirectory = new File(setDirectory, DIR_METADATA);
        resourceDirectory = new File(setDirectory, DIR_RESOURCES);
        capabilityListFile = new File(metadataDirectory, fileName);
        syncPropDirectory = new File(setDirectory, DIR_SYNC_PROPS);
        String syncDate = ZonedDateTimeUtil.toFileSaveFormat(syncStart);
        syncPropXmlFile = new File(syncPropDirectory, syncDate + ".xml");

        // Get the previously RsProperties file ...
        File[] prevSyncProps = syncPropDirectory.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                boolean accepted = false;
                String filename = pathname.getName();
                if (filename.endsWith(".xml")) {
                    // only include fully synchronized runs.
                    RsProperties syncProps = new RsProperties();
                    try {
                        syncProps.loadFromXML(pathname);
                        accepted = syncProps.getBool(Sync.PROP_SW_FULLY_SYNCHRONIZED);
                    } catch (IOException e) {
                        logger.warn("Could not read syncProps from {}", pathname, e);
                    }
                }
                return accepted;
            }
        });
        if (prevSyncProps != null && prevSyncProps.length > 0) {
            List<File> prevSyncPropList = Arrays.asList(prevSyncProps);
            Collections.sort(prevSyncPropList);
            prevSyncPropXmlFile = prevSyncPropList.get(prevSyncPropList.size() - 1);
        } else {
            prevSyncPropXmlFile = null;
        }

        logger.info("Created path finder with syncStart {} for {}", syncStart, capabilityListUri);
    }

    public ZonedDateTime getSyncStart() {
        return syncStart;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getPath() {
        return path;
    }

    public URI getCapabilityListUri() {
        return capabilityListUri;
    }

    public File getBaseDirectory() {
        return baseDirectory;
    }

    public File getSetDirectory() {
        return setDirectory;
    }

    public File getMetadataDirectory() {
        return metadataDirectory;
    }

    public File getResourceDirectory() {
        return resourceDirectory;
    }

    public File getSyncPropDirectory() {
        return syncPropDirectory;
    }

    public File getSyncPropXmlFile() {
        return syncPropXmlFile;
    }

    public File getPrevSyncPropXmlFile() {
        return prevSyncPropXmlFile;
    }

    public File getCapabilityListFile() {
        return capabilityListFile;
    }

    public File getDescriptionFile(@Nullable URI uri) {
        if (descriptionFile == null) {
            String filename = "";
            if (uri != null) {
                Optional<URI> maybeNormalized = NormURI.normalize(uri);
                if (maybeNormalized.isPresent()) {
                    filename = FilenameUtils.getName(maybeNormalized.get().getPath());
                }
            }
            if (filename.equals("")) {
                filename = "description";
            }
            descriptionFile = new File(metadataDirectory, filename);
        }
        return descriptionFile;
    }

    public File findMetadataFilePath(@Nonnull URI uri) {
        String restPath = extractPath(uri).replace(path, "");
        return new File(metadataDirectory, restPath);
    }

    public File findResourceFilePath(@Nonnull URI uri) {
        return new File(resourceDirectory, extractPath(uri));
    }

    private String extractPath(@Nonnull URI uri) {
        URI otherUri = NormURI.normalize(uri).orElse(null);
        String otherHost = otherUri.getHost();
        int otherPort = otherUri.getPort();
        if (!host.equals(otherHost)) {
            throw new IllegalArgumentException(
              "Normalized host names unequal. this host:" + host + " other: " + otherHost);
        } else if (port != otherPort) {
            throw new IllegalArgumentException("Ports unequal. this port:" + port + " other: " + otherPort);
        }
        return otherUri.getPath();
    }

    public Set<File> findResourceFilePaths(Collection<URI> uris) {
        return uris.parallelStream()
                   .map(this::findResourceFilePath).collect(Collectors.toSet());
    }

    public Set<File> findMetadataFilePaths(Collection<URI> uris) {
        Set<File> files = uris.parallelStream()
                              .map(this::findMetadataFilePath).collect(Collectors.toSet());
        if (descriptionFile != null) {
            files.add(descriptionFile);
        }
        return files;
    }

}
