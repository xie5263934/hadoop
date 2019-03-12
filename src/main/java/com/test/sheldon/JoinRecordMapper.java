package com.test.sheldon;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\\t");
        if (val.length == 3) {
            context.write(new TextPair(val[0], "1"), new Text(val[1] + "\t" + val[2]));
        }
    }
}
