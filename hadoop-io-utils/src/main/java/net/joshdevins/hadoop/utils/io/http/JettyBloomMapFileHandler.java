package net.joshdevins.hadoop.utils.io.http;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.joshdevins.hadoop.utils.io.Pair;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.BloomMapFile.Reader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;

import com.google.common.collect.MapMaker;

/**
 * A {@link Handler} for embedded Jetty to serve files out of {@link BloomMapFile}s. This currently assumes no
 * sub-driectories will ever be accessed since it splits the request URL into two parts: {dataset path}/{file name
 * {@link BloomMapFile}. This is pretty simple in that it will just iterate over all the bloom filters for that dataset
 * and test for the file. Not efficient, but simple.
 * 
 * @author Josh Devins
 */
public class JettyBloomMapFileHandler extends AbstractJettyHdfsFileHandler {

    private final ConcurrentMap<String, Set<BloomMapFile.Reader>> datasetMap;

    public JettyBloomMapFileHandler(final String rootPathInFileSystem) throws IOException {
        super(rootPathInFileSystem);

        // build an entry expiring (based on access) ConcurrentHashMap with soft referenced values
        // this will enable garbage collection to sweep away values at will
        // this will also do some pre-emptive cleaning if a dataset has not been used recently
        datasetMap = new MapMaker().softValues().expireAfterAccess(3, TimeUnit.DAYS).makeMap();
    }

    @Override
    protected void handleWithExceptionTranslation(final String target, final Request baseRequest,
            final HttpServletRequest request, final HttpServletResponse response) {

        // split the target URL into two parts
        Pair<String, String> splitTarget = splitTargetIntoDatasetAndFilename(target);
        if (splitTarget == null) {
            throw new HttpErrorException(HttpServletResponse.SC_NOT_ACCEPTABLE,
                    "Error splitting target into dataset and filename: " + target);
        }

        String dataset = splitTarget.getA();
        String filename = splitTarget.getB();
        String datasetFilenameDebugString = "dataset=" + dataset + " filename=" + filename;

        // get the readers for this dataset
        Set<BloomMapFile.Reader> readers = datasetMap.get(dataset);

        // need to get the readers
        if (readers == null) {
            readers = getReadersForDataset(dataset);

            // only need to set it if it still doesn't exist (race conditions, needs fixing?)
            datasetMap.putIfAbsent(dataset, readers);
        }

        // have the readers, find the file
        BytesWritable value = new BytesWritable();
        Text key = new Text(filename);
        boolean found = false;

        for (BloomMapFile.Reader reader : readers) {

            // check the bloom filter first, then try to get from the mapfile
            try {
                try {
                    if (reader.probablyHasKey(key) && reader.get(key, value) != null) {
                        found = true;
                        break;
                    }

                } finally {
                    IOUtils.closeStream(reader);
                }
            } catch (IOException ioe) {
                throw new HttpErrorException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        "Error looking for filename key in mapfile reader: " + datasetFilenameDebugString, ioe);
            }
        }

        // not found? need this since value is already non-null
        if (!found) {
            throw new HttpErrorException(HttpServletResponse.SC_NOT_FOUND,
                    "File was not found in any backing mapfile: " + datasetFilenameDebugString);
        }

        // get the bytes out of the value, trimmed padding
        byte[] bytes = new byte[value.getLength()];
        System.arraycopy(value.getBytes(), 0, bytes, 0, value.getLength());

        // send response with hopefully right content/mime type
        response.setContentType(getMimeType(filename));
        response.setStatus(HttpServletResponse.SC_OK);
        try {
            response.getOutputStream().write(bytes);
        } catch (IOException ioe) {
            throw new HttpErrorException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Error writing file bytes to output stream", ioe);
        }

        ((Request) request).setHandled(true);
    }

    Set<Reader> getReadersForDataset(final String dataset) {

        Set<Reader> readers = new HashSet<BloomMapFile.Reader>();

        // verify dataset
        Path datasetPath = new Path(getRootPathInFileSystem() + dataset);
        try {
            if (!getFileSystem().exists(datasetPath) || !getFileSystem().getFileStatus(datasetPath).isDir()) {

                throw new HttpErrorException(HttpServletResponse.SC_NOT_FOUND, "Dataset directory does not exist: "
                        + dataset);
            }

        } catch (IOException ioe) {
            throw new HttpErrorException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Could not open dataset directory: " + dataset, ioe);
        }

        // get files in dir
        FileStatus[] files;
        try {
            files = getFileSystem().listStatus(datasetPath);

        } catch (IOException ioe) {
            throw new HttpErrorException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Could not list map files in dataset: " + dataset, ioe);
        }

        for (FileStatus fileStatus : files) {

            // skip any sub-directories for now
            if (fileStatus.isDir()) {
                continue;
            }

            Path path = fileStatus.getPath();
            try {
                readers.add(new BloomMapFile.Reader(getFileSystem(), path.toString(), getConfiguration()));

            } catch (IOException ioe) {
                throw new HttpErrorException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        "Could not create reader for map file: " + path.toString(), ioe);
            }
        }

        return readers;
    }

    Pair<String, String> splitTargetIntoDatasetAndFilename(final String target) {

        // break the request URI into two parts: dataset path, filename in map file
        int splitAt = StringUtils.lastIndexOf(target, '/');

        // ensure split is possible
        if (splitAt < 1 || splitAt == target.length() - 1) {
            return null;
        }

        String dataset = target.substring(0, splitAt);
        String filename = target.substring(splitAt + 1);

        return new Pair<String, String>(dataset, filename);
    }
}
