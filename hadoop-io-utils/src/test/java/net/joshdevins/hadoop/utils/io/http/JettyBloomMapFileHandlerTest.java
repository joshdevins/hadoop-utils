package net.joshdevins.hadoop.utils.io.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.joshdevins.hadoop.utils.io.FileUtils;
import net.joshdevins.hadoop.utils.io.Pair;
import net.joshdevins.hadoop.utils.io.converter.FilesIntoBloomMapFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFileWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.eclipse.jetty.server.Request;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JettyBloomMapFileHandlerTest {

    private static final String TEST_OUTPUT = "target/test/output/JettyBloomMapFileHandlerTest";

    private static final String TEST_BLOOMMAPFILE = TEST_OUTPUT + "/dataset/bloom.map";

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
        FilesIntoBloomMapFile runner = new FilesIntoBloomMapFile("src/test/resources/input", TEST_BLOOMMAPFILE);
        runner.run();
    }

    @Test
    public void testHandleWithExceptionTranslation_DELETE() throws IOException {

        ByteArrayOutputStream baos = setupMockOutputStream();

        // initial request
        Mockito.when(mockRequest.getMethod()).thenReturn("GET");

        handler.handleWithExceptionTranslation("/dataset/0.txt", baseRequest, mockRequest, mockResponse);
        Assert.assertEquals("Contents of file 0", baos.toString());
        baos.reset();

        // cleanup
        Mockito.when(mockRequest.getMethod()).thenReturn("DELETE");
        handler.handleWithExceptionTranslation("/dataset", baseRequest, mockRequest, mockResponse);

        // change the file in the backing BloomMapFile and see if the new contents are loaded
        Configuration conf = new Configuration();
        BloomMapFileWriter writer = new BloomMapFileWriter(conf, new Path(TEST_BLOOMMAPFILE).getFileSystem(conf),
                TEST_BLOOMMAPFILE, Text.class, BytesWritable.class, CompressionType.NONE);
        writer.append(new Text("0.txt"), new BytesWritable("NEW Contents of file 0".getBytes()));
        IOUtils.closeStream(writer);

        // next should work again
        Mockito.when(mockRequest.getMethod()).thenReturn("GET");

        handler.handleWithExceptionTranslation("/dataset/0.txt", baseRequest, mockRequest, mockResponse);
        Assert.assertEquals("NEW Contents of file 0", baos.toString());
        baos.reset();

        Mockito.verify(mockResponse, Mockito.times(3)).setStatus(HttpServletResponse.SC_OK);
    }

    @Test
    public void testHandleWithExceptionTranslation_GET() throws IOException {

        ByteArrayOutputStream baos = setupMockOutputStream();

        Mockito.when(mockRequest.getMethod()).thenReturn("GET");

        // this sequence of file gets causes last file to not be found in the bloom filter from a Hadoop bug
        handler.handleWithExceptionTranslation("/dataset/1.txt", baseRequest, mockRequest, mockResponse);
        Assert.assertEquals("Contents of file 1", baos.toString());
        baos.reset();

        handler.handleWithExceptionTranslation("/dataset/file.jpg", baseRequest, mockRequest, mockResponse);
        baos.reset();

        handler.handleWithExceptionTranslation("/dataset/1.txt", baseRequest, mockRequest, mockResponse);
        Assert.assertEquals("Contents of file 1", baos.toString());
        baos.reset();

        Mockito.verify(mockResponse, Mockito.times(3)).setStatus(HttpServletResponse.SC_OK);
    }

    @Test
    public void testHandleWithExceptionTranslation_GET_NotFound() {

        Mockito.when(mockRequest.getMethod()).thenReturn("GET");

        try {
            handler.handleWithExceptionTranslation("/dataset/foo.txt", baseRequest, mockRequest, mockResponse);
            Assert.fail("Expected exception");

        } catch (HttpErrorException hee) {
            Assert.assertEquals(404, hee.getStatusCode());
            Assert.assertTrue(!hee.getMessage().contains("cached"));
        }

        try {
            handler.handleWithExceptionTranslation("/dataset/foo.txt", baseRequest, mockRequest, mockResponse);
            Assert.fail("Expected exception");

        } catch (HttpErrorException hee) {
            Assert.assertEquals(404, hee.getStatusCode());
            Assert.assertTrue(hee.getMessage().contains("cached"));
        }
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

    private ByteArrayOutputStream setupMockOutputStream() throws IOException {

        // setup output stream
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ServletOutputStream os = new ServletOutputStream() {

            @Override
            public void write(final int b) throws IOException {
                baos.write(b);
            }
        };

        Mockito.when(mockResponse.getOutputStream()).thenReturn(os);

        return baos;
    }
}
