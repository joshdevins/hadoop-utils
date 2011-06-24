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

    private static String USAGE = "Usage: HttpHdfsFileServer <port>";

    public static void main(final String[] args) {

        Validate.notNull(args, USAGE);
        Validate.isTrue(args.length == 2, USAGE);

        try {
            run(Integer.parseInt(args[0]), args[1]);
        } catch (ExitException ee) {
            // abnormal exit
            System.exit(1);
        }
    }

    static void run(final int port, final String rootPathInFileSystem) {

        Handler handler = null;
        try {
            // uses a BloomMapFile backing by default
            handler = new JettyBloomMapFileHandler(rootPathInFileSystem);
        } catch (IOException ioe) {
            MainUtils.exitWithStackTraceAndError("Error starting up HTTP server handler", ioe);
        }

        Server server = new Server(port);
        server.setHandler(handler);

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
}
