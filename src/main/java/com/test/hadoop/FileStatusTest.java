package com.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;


public class FileStatusTest {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        FileStatus status = fs.getFileStatus(new Path(args[0]));
        System.out.println(status.getOwner());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(simpleDateFormat.format(new Date(status.getModificationTime())));
        System.out.println(simpleDateFormat.format(new Date(status.getAccessTime())));
        System.out.println(status.getOwner());
        System.out.println(status.getGroup());
    }
}
