package net.joshdevins.hadoop.utils.io;

import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;

/**
 * A rudimentary HDFS-based, HTTP file server. This will use a set of backing {@link MapFile}s and optionally an
 * external file to {@link MapFile} index to serve files embedded in the values of the {@link MapFile}.
 * 
 * <p>
 * The basic premise is as follows. Serving small files (like images) out of HDFS is pretty much a no-go given the
 * amount of overhead involved in just storing and managing the little files. We first encountered this when building an
 * in-house map-tile server to serve tiles built in Hadoop. To work around this problem, we store a whole bunch of files
 * in multiple {@link MapFile}s (generally a {@link MapFile} per reducer). This server will then take an index of
 * "filename" -> "MapFile filename" then do another "filename" lookup on the {@link MapFile}'s internal index to find
 * the offset in the {@link MapFile}'s backing data {@link SequenceFile}. The file that is returned from the server will
 * have a MIME type based on the file extension in the {@link MapFile}'s key.
 * </p>
 * 
 * <p>
 * Internally this relies on a couple of caching mechanisms for the index. Firstly, the primary indices of "filename" ->
 * "MapFile filename" is stored in memory. The secondary indicies are stored in memory by the {@link MapFile.Reader}
 * itself. We don't use a {@link BloomMapFile} in this case because we already have the primary index which guarntees
 * that the {@link MapFile} we access has the key we are looking for.
 * </p>
 * 
 * @see JettyMapFileHandler JettyMapFileHandler (for details on directory layouts, index formats, etc.)
 * @see FilesIntoSequenceFile
 * @see SequenceFileToMapFile
 * 
 * @author Josh Devins
 */
public class HttpMapFileServer {

    private static String USAGE = "Usage: HttpMapFileServer <port>";

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

    private static void run(final int port, final String rootPathInFileSystem) {

        Handler handler = null;
        try {
            handler = new JettyMapFileHandler(rootPathInFileSystem);
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
