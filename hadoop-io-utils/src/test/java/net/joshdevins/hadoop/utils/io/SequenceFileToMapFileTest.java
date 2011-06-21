package net.joshdevins.hadoop.utils.io;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SequenceFileToMapFileTest {

    private static final String[] TEST_FILES = new String[] { "0.txt", "1.txt", "2.txt" };

    private static String WORK_DIR = "target/test/output/SequenceFileToMapFileTest/";

    private static String TEST_INPUT = WORK_DIR + "sequencefiles/part-r-00000";

    private static String TEST_OUTPUT = WORK_DIR + "mapfiles/00000";

    @After
    @Before
    public void cleanupWorkDir() {
        FileUtils.deleteDirectory(WORK_DIR);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadArgs() {
        FilesIntoSequenceFile.run(null, null);
    }

    @Test
    public void testRun() throws IOException {

        // build a SequenceFile from a bunch of text files
        FilesIntoSequenceFile.run("src/test/resources/input", TEST_INPUT);

        // convert it to a MapFile
        SequenceFileToMapFile.run(TEST_INPUT, TEST_OUTPUT);

        // verify MapFile contents
        Map<String, String> map = readMapFile(TEST_OUTPUT, TEST_FILES);

        Assert.assertEquals(TEST_FILES.length, map.size());

        for (int i = 0; i < TEST_FILES.length; i++) {

            String key = TEST_FILES[i];
            Assert.assertTrue(map.containsKey(key));
            Assert.assertEquals("Contents of file " + i, map.get(key));
        }
    }

    private Map<String, String> readMapFile(final String uri, final String... keys) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        MapFile.Reader reader = null;
        Map<String, String> map = new HashMap<String, String>();

        try {
            reader = new MapFile.Reader(fs, uri, conf);
            BytesWritable value = new BytesWritable();

            for (String key : keys) {

                Writable val = reader.get(new Text(key), value);

                if (val == null) {
                    // not found, just skip it
                    continue;
                }

                byte[] bytes = new byte[value.getLength()];
                System.arraycopy(value.getBytes(), 0, bytes, 0, value.getLength());

                map.put(key, new String(bytes));
            }

        } finally {
            IOUtils.closeStream(reader);
        }

        return map;
    }
}
