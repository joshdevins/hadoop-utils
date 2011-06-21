package net.joshdevins.hadoop.utils.io;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.activation.MimetypesFileTypeMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * A {@link Handler} for embedded Jetty to serve files out of {@link MapFile}s.
 * 
 * <p>
 * Assumptions and expectations:
 * <ul>
 * <li>It is assumed that the indices and {@link MapFile}s directories are both on the same {@link FileSystem} beside
 * each other.</li>
 * <li>Sub-directories are not recursed into, they are simply ignored.</li>
 * <li>Index files are {@link SequenceFile}s with key the filename, value the {@link MapFile} filename where the data
 * lives.</li>
 * </ul>
 * 
 * <p>
 * Layout of directories for indices and {@link MapFile}s in the filesystem <b>must</b> be as follows (filenames are
 * arbitrary/can be opaque), for an example dataset called "test":
 * </p>
 * 
 * <pre>
 * test/indices/index-00000
 * test/indices/index-00001
 * test/indices/index-00002
 * ...
 * test/mapfiles/mapfile-00001/index
 * test/mapfiles/mapfile-00001/data
 * test/mapfiles/mapfile-00002/index
 * test/mapfiles/mapfile-00002/data
 * ...
 * </pre>
 * 
 * <p>
 * Contents of the indices would then look something like:
 * </p>
 * 
 * <pre>
 * image-00000.png  mapfile-00001
 * image-00001.png  mapfile-00002
 * image-00002.png  mapfile-00001
 * image-00003.png  mapfile-00001
 * image-00004.png  mapfile-00002
 * ...
 * </pre>
 * 
 * <p>
 * All of the above directories and files should be relative to the root path in the filesystem. For example:
 * <code>hdfs://<namenode>:<port>/user/devins/http</code>
 * </p>
 * 
 * <p>
 * Indices are loaded lazily but you can trigger a load by going to the root URLs. In the above example, to load the
 * full index for dataset "test", you would visit the following URL: <code>http://<server>:<port>/test</code>.
 * </p>
 * 
 * @see HttpMapFileServer
 * 
 * @author Josh Devins
 */
public class JettyMapFileHandler extends AbstractHandler implements Handler {

    private final String rootPathInFileSystem;

    private final Configuration conf;

    private final FileSystem fileSystem;

    private final Map<String, Map<String, String>> primaryIndices;

    /**
     * Default constructure.
     * 
     * @throws IOException
     *         Thrown from underlying {@link FileSystem#get(Configuration)} call.
     */
    public JettyMapFileHandler(final String rootPathInFileSystem) throws IOException {

        Validate.notEmpty(rootPathInFileSystem, "Root path in filesystem is required");

        conf = new Configuration();
        fileSystem = FileSystem.get(URI.create(rootPathInFileSystem), conf);

        // TODO: ensure this exists at least at startup here
        this.rootPathInFileSystem = rootPathInFileSystem;
        this.primaryIndices = new ConcurrentHashMap<String, Map<String, String>>();
    }

    @Override
    public void handle(final String target, final Request baseRequest, final HttpServletRequest request,
            final HttpServletResponse response) throws IOException, ServletException {

        // partition the target into two parts: dataset path and filename

        // // lookup target in primary index
        // if (!primaryIndex.containsKey(target)) {
        // fileNotFound(request, response);
        // return;
        // }
        //
        // String mapFileFilename = primaryIndex.get(target);
        //
        // // lookup the file bytes in the MapFile
        // byte[] bytes = readFileFromMapFile(target, mapFileFilename);
        //
        // response.setContentType(getMimeType(target));
        // response.setStatus(HttpServletResponse.SC_OK);
        //
        // response.getOutputStream().write(bytes);
        // response.getOutputStream().flush();
        //
        // ((Request) request).setHandled(true);

        fileNotFound(request, response);
    }

    Map<String, String> buildPrimaryIndex(final String dataset) throws IOException {

        Path indexFilesPath = new Path(rootPathInFileSystem + "/" + dataset + "/indices");

        // ensure path exists and is dir
        if (!fileSystem.getFileStatus(indexFilesPath).isDir()) {
            throw new IllegalArgumentException("Index files directory is not a directory! " + indexFilesPath.getName());
        }

        Map<String, String> index = new HashMap<String, String>();

        // get files in dir
        FileStatus[] files = fileSystem.listStatus(indexFilesPath);
        for (FileStatus fileStatus : files) {

            // skip any sub-directories for now
            if (fileStatus.isDir()) {
                continue;
            }

            Path path = fileStatus.getPath();
            try {
                loadPrimaryIndexFile(path, index);
            } catch (IOException ioe) {
                System.err.println("Error reading primary index SequenceFile: " + path.getName());
            }
        }

        return index;
    }

    String getMimeType(final String filename) {

        MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
        return mimeTypesMap.getContentType(filename);
    }

    void loadPrimaryIndexFile(final Path path, final Map<String, String> index) throws IOException {

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fileSystem, path, conf);
            Text key = new Text();
            BytesWritable value = new BytesWritable();

            while (reader.next(key, value)) {

                byte[] bytes = new byte[value.getLength()];
                System.arraycopy(value.getBytes(), 0, bytes, 0, value.getLength());

                index.put(key.toString(), new String(bytes));
            }

        } finally {
            IOUtils.closeStream(reader);
        }
    }

    byte[] readBytesFromMapFile(final String key, final String dataset, final String mapFileFilename)
            throws IOException {

        MapFile.Reader reader = null;
        try {
            reader = new MapFile.Reader(fileSystem, rootPathInFileSystem + "/" + dataset + "/mapfiles/"
                    + mapFileFilename, conf);
            BytesWritable value = new BytesWritable();
            Writable val = reader.get(new Text(key), value);

            if (val == null) {
                // not found
                return null;
            }

            byte[] bytes = new byte[value.getLength()];
            System.arraycopy(value.getBytes(), 0, bytes, 0, value.getLength());
            return bytes;

        } finally {
            if (reader != null) {
                IOUtils.closeStream(reader);
            }
        }
    }

    private void fileNotFound(final HttpServletRequest request, final HttpServletResponse response) throws IOException {

        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        response.getWriter().println("File not found");

        ((Request) request).setHandled(true);
    }
}
