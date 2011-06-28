package net.joshdevins.hadoop.utils.io.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import net.joshdevins.hadoop.utils.io.FileUtils;
import net.joshdevins.hadoop.utils.io.FilesIntoBloomMapFile;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HttpHdfsFileServerTest {

    private static class Runner extends Thread {

        @Override
        public void run() {
            // TODO: port to run on should be randomized
            HttpHdfsFileServer.run(TEST_PORT, TEST_ROOT);
        }
    }

    private static final int TEST_PORT = 8645;

    private static final String TEST_ROOT = "target/test/output/HttpHdfsFileServerTest";

    private Runner runner;

    @SuppressWarnings("deprecation")
    @After
    public void after() {
        // kill runner, this should be more graceful
        runner.stop();
    }

    @Before
    public void before() throws InterruptedException {

        // create BloomMapFiles from plain text files
        FileUtils.createDirectoryDestructive(TEST_ROOT);
        FilesIntoBloomMapFile setup = new FilesIntoBloomMapFile("src/test/resources/input", TEST_ROOT
                + "/dataset/bloom.map");
        setup.run();

        // run the server
        runner = new Runner();
        runner.start();
        Thread.sleep(1000);
    }

    public String makeHttpGetRequest(final String path) throws IOException {

        URL url = new URL("http://localhost:" + TEST_PORT + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("GET");
        connection.setDoOutput(true);
        connection.setReadTimeout(10000);

        connection.connect();

        // get response
        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        StringBuilder sb = new StringBuilder();
        String line = null;

        while ((line = br.readLine()) != null) {
            sb.append(line);
        }

        return sb.toString();
    }

    @Test
    public void testSuccess() throws Exception {

        Assert.assertEquals("Contents of file 0", makeHttpGetRequest("/dataset/0.txt"));
        Assert.assertEquals("Contents of file 1", makeHttpGetRequest("/dataset/1.txt"));
    }
}
