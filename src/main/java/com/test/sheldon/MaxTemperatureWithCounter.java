package com.test.sheldon;

import com.test.util.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureWithCounter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setJarByClass(getClass());
        job.setMapperClass(MaxTemperatureMapperWithCounter.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        int code = job.waitForCompletion(true) ? 0 : -1;
        if (0 == code) {
            System.out.println("User count:");
            System.out.println("TemperatueEnum.MALFORMED=" + job.getCounters().findCounter(TemperatueEnum.MALFORMED).getValue());
            System.out.println("TemperatueEnum.MISSING=" + job.getCounters().findCounter(TemperatueEnum.MISSING).getValue());
        }
        return code;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
        System.exit(exitCode);
    }
}
