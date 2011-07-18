package net.joshdevins.hadoop.utils.io.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import net.joshdevins.hadoop.utils.MainUtils;
import net.joshdevins.hadoop.utils.io.FileUtils;
import net.joshdevins.hadoop.utils.io.IOUtils;
import net.joshdevins.hadoop.utils.io.converter.FilesIntoBloomMapFile;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HttpHdfsFileServerTest {

    private static class Runner extends Thread {

        private final HttpHdfsFileServer server;

        private Runner(final HttpHdfsFileServer server) {
            this.server = server;
        }

        @Override
        public void run() {
            server.run();
        }
    }

    private static final String TEST_ROOT = "target/test/output/HttpHdfsFileServerTest";

    private HttpHdfsFileServer server;

    private Runner runner;

    private int port;

    @After
    public void after() throws Exception {
        server.getJettyServer().stop();
        Thread.sleep(100);
    }

    @Before
    public void before() throws Exception {

        // create BloomMapFiles from plain text files
        FileUtils.createDirectoryDestructive(TEST_ROOT);
        MainUtils.toolRunnerWithoutExit(new FilesIntoBloomMapFile(), new String[] { "src/test/resources/input/files",
                TEST_ROOT + "/dataset/bloom.map" });

        // create the server
        port = IOUtils.getRandomUnusedPort();
        server = new HttpHdfsFileServer(port, TEST_ROOT);

        // run the server
        runner = new Runner(server);
        runner.start();

        while (server.getJettyServer().isStarting()) {
            Thread.sleep(100);
        }

        // for good measure
        Thread.sleep(100);
    }

    public String makeHttpGetRequest(final String path) throws IOException {

        InputStream is = makeHttpGetRequestRaw(path);

        try {
            byte[] bytes = IOUtils.getBytesFromInputStream(is);
            return new String(bytes);
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    public InputStream makeHttpGetRequestRaw(final String path) throws IOException {

        URL url = new URL("http://localhost:" + port + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("GET");
        connection.setDoOutput(true);
        connection.setReadTimeout(10000);

        connection.connect();

        if (connection.getResponseCode() >= 400) {
            return connection.getErrorStream();
        }

        return connection.getInputStream();
    }

    @Test
    public void testCustom404() throws Exception {

        byte[] expected = IOUtils.getBytesFromResource("/images/black.png");
        byte[] actual = IOUtils.getBytesFromInputStream(makeHttpGetRequestRaw("/dataset/foo.txt?404=black"));

        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testNormal404() throws Exception {
        Assert.assertTrue(makeHttpGetRequest("/dataset/foo.txt").contains("404"));
    }

    @Test
    public void testSuccess() throws Exception {

        Assert.assertEquals("Contents of file 0", makeHttpGetRequest("/dataset/0.txt"));
        Assert.assertEquals("Contents of file 1", makeHttpGetRequest("/dataset/1.txt"));
    }
}
