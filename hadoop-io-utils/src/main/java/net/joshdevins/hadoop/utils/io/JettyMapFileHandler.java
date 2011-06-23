package net.joshdevins.hadoop.utils.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;

/**
 * INCOMPLETE IMPLEMENTATION! A {@link Handler} for embedded Jetty to serve files out of {@link MapFile}s.
 * 
 * <p>
 * This will use a set of backing {@link MapFile}s and an external file to {@link MapFile} index to serve files embedded
 * in the values of the {@link MapFile}.
 * </p>
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
 * @see HttpHdfsFileServer
 * 
 * @author Josh Devins
 */
public class JettyMapFileHandler extends AbstractJettyHdfsFileHandler {

    private final Map<String, Map<String, String>> primaryIndices;

    /**
     * Default constructure.
     * 
     * @throws IOException
     *         Thrown from underlying {@link FileSystem#get(Configuration)} call.
     */
    public JettyMapFileHandler(final String rootPathInFileSystem) throws IOException {

        super(rootPathInFileSystem);
        this.primaryIndices = new ConcurrentHashMap<String, Map<String, String>>();
    }

    @Override
    public void handleWithExceptionTranslation(final String target, final Request baseRequest,
            final HttpServletRequest request, final HttpServletResponse response) {

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
    }

    Map<String, String> buildPrimaryIndex(final String dataset) throws IOException {

        Path indexFilesPath = new Path(getRootPathInFileSystem() + "/" + dataset + "/indices");

        // ensure path exists and is dir
        if (!getFileSystem().getFileStatus(indexFilesPath).isDir()) {
            throw new IllegalArgumentException("Index files directory is not a directory! " + indexFilesPath.getName());
        }

        Map<String, String> index = new HashMap<String, String>();

        // get files in dir
        FileStatus[] files = getFileSystem().listStatus(indexFilesPath);
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

    void loadPrimaryIndexFile(final Path path, final Map<String, String> index) throws IOException {

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(getFileSystem(), path, getConfiguration());
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
            reader = new MapFile.Reader(getFileSystem(), getRootPathInFileSystem() + "/" + dataset + "/mapfiles/"
                    + mapFileFilename, getConfiguration());
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
}
