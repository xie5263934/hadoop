package com.test.sheldon;

import com.test.util.Parser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTempMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
    private Parser parser = new Parser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value.toString());
        if (parser.isValid()) {
            context.write(new IntPair(Integer.valueOf(parser.getYear()), parser.getTemperature()), NullWritable.get());
        }
    }
}
