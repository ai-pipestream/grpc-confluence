package ai.pipestream.okf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Packs an {@link OkfBundle} as a zip archive (OKF §3 distribution form).
 */
public final class OkfZip {

    private OkfZip() {
    }

    /**
     * Writes the bundle to {@code zipFile}.
     *
     * @param bundle the bundle
     * @param zipFile destination zip
     * @return {@code zipFile}
     * @throws IOException if the zip cannot be written
     */
    public static Path write(OkfBundle bundle, Path zipFile) throws IOException {
        Objects.requireNonNull(bundle, "bundle");
        Objects.requireNonNull(zipFile, "zipFile");
        if (zipFile.getParent() != null) {
            Files.createDirectories(zipFile.getParent());
        }
        try (OutputStream out = Files.newOutputStream(zipFile);
                ZipOutputStream zip = new ZipOutputStream(out)) {
            for (Map.Entry<String, byte[]> entry : bundle.files().entrySet()) {
                ZipEntry zipEntry = new ZipEntry(entry.getKey());
                zip.putNextEntry(zipEntry);
                zip.write(entry.getValue());
                zip.closeEntry();
            }
        }
        return zipFile;
    }
}
