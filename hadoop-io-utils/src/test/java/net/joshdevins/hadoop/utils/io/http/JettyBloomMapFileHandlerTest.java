package net.joshdevins.hadoop.utils.io.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.joshdevins.hadoop.utils.io.FileUtils;
import net.joshdevins.hadoop.utils.io.FilesIntoBloomMapFile;
import net.joshdevins.hadoop.utils.io.Pair;

import org.eclipse.jetty.server.Request;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JettyBloomMapFileHandlerTest {

    private static final String TEST_OUTPUT = "target/test/output/JettyBloomMapFileHandlerTest";

    private JettyBloomMapFileHandler handler;

    private Request baseRequest;

    private HttpServletRequest mockRequest;

    private HttpServletResponse mockResponse;

    @Before
    public void before() throws IOException {

        FileUtils.createDirectoryDestructive(TEST_OUTPUT);
        handler = new JettyBloomMapFileHandler(TEST_OUTPUT);

        // mocks
        baseRequest = new Request();
        mockRequest = Mockito.mock(Request.class);
        mockResponse = Mockito.mock(HttpServletResponse.class);

        // move text files into BloomMapFile in test output directory
        FilesIntoBloomMapFile.run("src/test/resources/input", TEST_OUTPUT + "/dataset/bloom.map");
    }

    @Test
    public void testHandleWithExceptionTranslation() throws IOException {

        // setup output stream
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ServletOutputStream os = new ServletOutputStream() {

            @Override
            public void write(final int b) throws IOException {
                baos.write(b);
            }
        };
        Mockito.when(mockResponse.getOutputStream()).thenReturn(os);

        handler.handleWithExceptionTranslation("/dataset/1.txt", baseRequest, mockRequest, mockResponse);
        Assert.assertEquals("Contents of file 1", baos.toString());
    }

    @Test
    public void testSplitTarget() {

        Pair<String, String> pair = handler.splitTargetIntoDatasetAndFilename("dataset/a/b/filename.txt");
        Assert.assertEquals("dataset/a/b", pair.getA());
        Assert.assertEquals("filename.txt", pair.getB());
    }

    @Test
    public void testSplitTarget_Invalid() {
        Assert.assertNull(handler.splitTargetIntoDatasetAndFilename("filename.txt"));
        Assert.assertNull(handler.splitTargetIntoDatasetAndFilename("a/"));
        Assert.assertNull(handler.splitTargetIntoDatasetAndFilename("/b"));
        Assert.assertNull(handler.splitTargetIntoDatasetAndFilename("/"));
    }

    @Test
    public void testSplitTarget_Short() {

        Pair<String, String> pair = handler.splitTargetIntoDatasetAndFilename("a/b");
        Assert.assertEquals("a", pair.getA());
        Assert.assertEquals("b", pair.getB());
    }
}
