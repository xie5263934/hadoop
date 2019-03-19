package com.test.avro;

import com.test.util.JobBuilder;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroTemperature extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), strings);
        if (job == null) {
            return -1;
        }
        //为了解决在Hadoop集群中运行时我们使用的Avro版本和集群中Avro版本不一致的问题
        job.getConfiguration().setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputValueSchema(job, ParserUtil.getSchema());
        AvroJob.setOutputKeySchema(job, ParserUtil.getSchema());
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setMapperClass(AvroMapper.class);
        job.setReducerClass(AvroReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(ToolRunner.run(new AvroTemperature(), args));
    }
}
