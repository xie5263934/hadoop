package com.test.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class JobBuilder {
    public static Job parseInputAndOutput(Tool tool, Configuration configuration, String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf("%Usage:%s [generic options] <input> <output>\n", tool.getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return null;
        }
        Job job = Job.getInstance(configuration);
        job.setJarByClass(tool.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }
}
