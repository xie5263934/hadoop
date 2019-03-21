package com.test.parquet;

import com.test.avro.ParserUtil;
import com.test.util.JobBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

public class ParquetMapReducer extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), strings);
        if (job == null) {
            return -1;
        }
        job.getConfiguration().setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        job.setMapperClass(ParquetMapper.class);
        job.setMapOutputKeyClass(Void.class);
        job.setMapOutputValueClass(GenericRecord.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setSchema(job, ParserUtil.getAvroParquetSchema());
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(ToolRunner.run(new ParquetMapReducer(), args));
    }
}
