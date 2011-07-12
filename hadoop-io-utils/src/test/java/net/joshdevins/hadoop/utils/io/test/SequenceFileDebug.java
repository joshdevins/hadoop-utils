package net.joshdevins.hadoop.utils.io.test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import net.joshdevins.hadoop.utils.Pair;
import net.joshdevins.hadoop.utils.io.HdfsUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Test;

public class SequenceFileDebug {

    @Ignore
    @Test
    public void testPrintKeysInDirOfMapFiles() throws IOException {

        File[] mapfiles = new File("/Users/devins/Downloads/glowmaptiles").listFiles();

        for (File mapfile : mapfiles) {

            List<Pair<Text, BytesWritable>> contents = HdfsUtils.<Text, BytesWritable> readSequenceFile(mapfile
                    .getAbsolutePath() + "/data");
            for (Pair<Text, BytesWritable> pair : contents) {
                System.out.println(pair.getA().toString());
            }
        }
    }
}
