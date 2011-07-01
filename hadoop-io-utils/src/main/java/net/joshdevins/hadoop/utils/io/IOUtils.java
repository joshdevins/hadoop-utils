package net.joshdevins.hadoop.utils.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang.Validate;

/**
 * General IO utilities.
 * 
 * @author Josh Devins
 */
public final class IOUtils {

    private IOUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads bytes from the {@link InputStream}. The input {@link InputStream} will be closed regardless of exceptions.
     */
    public static byte[] getBytesFromInputStream(final InputStream is) throws IOException {

        Validate.notNull(is);
        BufferedInputStream bis = new BufferedInputStream(is);

        try {
            byte[] buffer = new byte[bis.available()];
            bis.read(buffer);

            return buffer;

        } finally {
            try {
                bis.close();
            } finally {
                is.close();
            }
        }
    }

    public static byte[] getBytesFromResource(final String resource) throws IOException {

        Validate.notEmpty(resource);
        return getBytesFromInputStream(FileUtils.class.getResourceAsStream(resource));
    }
}
