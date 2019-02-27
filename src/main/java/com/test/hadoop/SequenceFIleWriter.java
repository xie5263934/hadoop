package com.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

public class SequenceFIleWriter {
    private static final String[] DATA =
            {
                    "one,two ,buckle my shoe",
                    "three,four,shut the door",
                    "five,six,pick up sticks",
                    "seven,eight,lay them,straight",
                    "nine,ten,a big hen",
            };

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, path, key.getClass(), value.getClass());
        for (int i = 0; i < 100; i++) {
            key.set(100 - i);
            value.set(DATA[i % DATA.length]);
            System.out.printf("[%s]\t%s\t%s", writer.getLength(), key, value);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);
    }
}
