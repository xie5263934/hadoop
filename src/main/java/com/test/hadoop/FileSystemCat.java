package com.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);
        InputStream inputStream = null;
        inputStream = fileSystem.open(new Path(uri));
        IOUtils.copyBytes(inputStream, System.out, 4096, false);
        IOUtils.closeStream(inputStream);
    }
}
