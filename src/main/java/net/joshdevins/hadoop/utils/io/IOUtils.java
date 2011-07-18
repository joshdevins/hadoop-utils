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
     * Reads bytes from the {@link InputStream}.
     */
    public static byte[] getBytesFromInputStream(final InputStream is) throws IOException {

        Validate.notNull(is);
        BufferedInputStream bis = new BufferedInputStream(is);

        byte[] buffer = new byte[bis.available()];
        bis.read(buffer);

        return buffer;
    }

    public static byte[] getBytesFromResource(final String resource) throws IOException {

        Validate.notEmpty(resource);
        InputStream is = FileUtils.class.getResourceAsStream(resource);

        try {
            return getBytesFromInputStream(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
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
