package net.joshdevins.hadoop.utils.io.http;

import java.io.IOException;

import net.joshdevins.hadoop.utils.io.ExitException;
import net.joshdevins.hadoop.utils.io.MainUtils;

import org.apache.commons.lang.Validate;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;

/**
 * A rudimentary HDFS-based, HTTP file server.
 * 
 * @see JettyMapFileHandler JettyMapFileHandler (for details on directory layouts, index formats, etc.)
 * 
 * @author Josh Devins
 */
public class HttpHdfsFileServer {

    private static String USAGE = "Usage: HttpHdfsFileServer <port> <root path in filesystem>";

    private final Server server;

    public HttpHdfsFileServer(final int port, final String rootPathInFileSystem) {

        Validate.isTrue(port >= 0 && port <= 65535, USAGE);
        Validate.notEmpty(rootPathInFileSystem, USAGE);

        Handler handler = null;
        try {
            // uses a BloomMapFile backing by default
            handler = new JettyBloomMapFileHandler(rootPathInFileSystem);
        } catch (IOException ioe) {
            MainUtils.exitWithStackTraceAndError("Error starting up HTTP server handler", ioe);
        }

        server = new Server(port);
        server.setHandler(handler);
    }

    public Server getJettyServer() {
        return server;
    }

    public void run() {

        try {
            server.start();
        } catch (Exception e) {
            MainUtils.exitWithStackTraceAndError("Failure starting HTTP server", e);
        }

        try {
            server.join();
        } catch (InterruptedException ie) {
            MainUtils.exitWithStackTraceAndError("Server interrupted while running", ie);
        }
    }

    public static void main(final String[] args) {

        Validate.notNull(args, USAGE);
        Validate.isTrue(args.length == 2, USAGE);

        try {
            HttpHdfsFileServer server = new HttpHdfsFileServer(Integer.parseInt(args[0]), args[1]);
            server.run();
        } catch (ExitException ee) {
            // abnormal exit
            System.exit(1);
        }
    }
}
