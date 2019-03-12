package com.test.sheldon;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().split("\\t");
        if (val.length == 2) {
            context.write(new TextPair(val[0], "0"), new Text(val[1]));
        }
    }
}
