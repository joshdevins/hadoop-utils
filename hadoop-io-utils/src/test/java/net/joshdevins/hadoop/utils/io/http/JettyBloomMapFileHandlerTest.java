package net.joshdevins.hadoop.utils.io.http;

import java.io.IOException;

import net.joshdevins.hadoop.utils.io.FileUtils;
import net.joshdevins.hadoop.utils.io.Pair;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JettyBloomMapFileHandlerTest {

    private static final String TEST_OUTPUT = "target/test/output/JettyBloomMapFileHandlerTest";

    private JettyBloomMapFileHandler handler;

    @Before
    public void before() throws IOException {

        FileUtils.createDirectoryDestructive(TEST_OUTPUT);
        handler = new JettyBloomMapFileHandler(TEST_OUTPUT);
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
