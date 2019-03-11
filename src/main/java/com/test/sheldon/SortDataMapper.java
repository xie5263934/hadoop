package com.test.sheldon;

import com.test.util.Parser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortDataMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private Parser parser = new Parser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value.toString());
        if (parser.isValidTemperature()) {
            context.write(new IntWritable(parser.getAirTemperature()), value);
        }
    }
}
