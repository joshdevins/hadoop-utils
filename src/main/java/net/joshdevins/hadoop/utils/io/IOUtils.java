package net.joshdevins.hadoop.utils.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;

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

    /**
     * Gets a random, unused port.
     */
    public static int getRandomUnusedPort() {

        final int port;
        ServerSocket socket = null;

        try {
            socket = new ServerSocket(0);
            port = socket.getLocalPort();

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);

        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }
        }

        return port;
    }
}
