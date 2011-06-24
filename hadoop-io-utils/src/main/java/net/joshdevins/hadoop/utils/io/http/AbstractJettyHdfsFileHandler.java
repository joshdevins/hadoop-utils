package net.joshdevins.hadoop.utils.io.http;

import java.io.IOException;
import java.io.PrintWriter;

import javax.activation.MimetypesFileTypeMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Base class for all Jetty HDFS file {@link Handler}s.
 * 
 * @author Josh Devins
 */
public abstract class AbstractJettyHdfsFileHandler extends AbstractHandler implements Handler {

    private static MimetypesFileTypeMap MIME_TYPES_MAP = new MimetypesFileTypeMap();

    static {
        MIME_TYPES_MAP.addMimeTypes("image/png png PNG");
    }

    private final String rootPathInFileSystem;

    private final Configuration conf;

    private final FileSystem fileSystem;

    public AbstractJettyHdfsFileHandler(final String rootPathInFileSystem) throws IOException {

        Validate.notEmpty(rootPathInFileSystem, "Root path in filesystem is required");

        conf = new Configuration();

        Path rootPath = new Path(rootPathInFileSystem);
        fileSystem = rootPath.getFileSystem(conf);

        // check for root path, must be a dir
        Validate.isTrue(fileSystem.exists(rootPath), "Root path in filesystem does not exist: " + rootPathInFileSystem);

        FileStatus fileStatus = fileSystem.getFileStatus(rootPath);
        Validate.isTrue(fileStatus.isDir(), "Root path in filesystem is not a directory: " + rootPathInFileSystem);

        this.rootPathInFileSystem = rootPathInFileSystem;
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public String getRootPathInFileSystem() {
        return rootPathInFileSystem;
    }

    public void handle(final String target, final Request baseRequest, final HttpServletRequest request,
            final HttpServletResponse response) throws IOException, ServletException {

        try {
            handleWithExceptionTranslation(target, baseRequest, request, response);
        } catch (HttpErrorException hee) {

            response.setContentType("text/html");
            response.setStatus(hee.getStatusCode());

            String errorString = "Error " + hee.getStatusCode();

            PrintWriter writer = response.getWriter();
            writer.println("<html><head><title>" + errorString + "<title></head><body>");
            writer.println("<h2>" + errorString + "</h2>");

            if (!StringUtils.isBlank(hee.getMessage())) {
                writer.println("<p><b>");
                writer.println(hee.getMessage());
                writer.println("</b></p>");
            }

            if (hee.getCause() != null) {
                writer.println("<p>");
                writer.println(hee.getCause().getMessage() + "<br />");

                StackTraceElement[] stackTrace = hee.getCause().getStackTrace();
                for (StackTraceElement e : stackTrace) {
                    writer.println(e.toString() + "<br />");
                }

                writer.println("</p>");
            }

            writer.println("</body></html>");

            ((Request) request).setHandled(true);
        }
    }

    protected abstract void handleWithExceptionTranslation(final String target, final Request baseRequest,
            final HttpServletRequest request, final HttpServletResponse response);

    /**
     * A very simple way to check mime-type. If this is not good enough, use something like mime-utils or JMimeMagic.
     */
    public static String getMimeType(final String filename) {
        return MIME_TYPES_MAP.getContentType(filename);
    }
}
