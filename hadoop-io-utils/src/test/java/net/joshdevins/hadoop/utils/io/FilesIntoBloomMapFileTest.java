package net.joshdevins.hadoop.utils.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class FilesIntoBloomMapFileTest {

    private static final String TEST_OUTPUT = "target/test/output/FilesIntoBloomMapFileTest/file.map";

    @Test(expected = IllegalArgumentException.class)
    public void testBadArgs() {
        FilesIntoBloomMapFile.run(null, null);
    }

    @Test
    public void testRun() throws IOException {

        FilesIntoBloomMapFile.run("src/test/resources/input", TEST_OUTPUT);

        // test results
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(TEST_OUTPUT), conf);
        BloomMapFile.Reader reader = new BloomMapFile.Reader(fs, TEST_OUTPUT, conf);

        Assert.assertNotNull(reader);

        try {
            for (int i = 0; i < 3; i++) {

                Text key = new Text(i + ".txt");
                Assert.assertTrue(reader.probablyHasKey(key));

                BytesWritable value = new BytesWritable();
                reader.get(key, value);
                Assert.assertNotNull(value);

                byte[] bytes = new byte[value.getLength()];
                System.arraycopy(value.getBytes(), 0, bytes, 0, value.getLength());

                Assert.assertEquals("Contents of file " + i, new String(bytes));
            }

            Assert.assertNull(reader.get(new Text("foobar"), new BytesWritable()));

        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
