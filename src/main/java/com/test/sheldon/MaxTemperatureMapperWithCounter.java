package com.test.sheldon;

import com.test.util.Parser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Parser parser = new Parser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value.toString());
        if (parser.isValidTemperature()) {
            int temperature = parser.getAirTemperature();
            context.write(new Text(parser.getYear()), new IntWritable(temperature));
        } else if (parser.isMalformedTemperature()) {
            System.err.println("Ignoring possibly corrupt input :" + value);
            context.getCounter(TemperatueEnum.MALFORMED).increment(1);
        } else if (parser.isMissingTemperature()) {
            context.getCounter(TemperatueEnum.MISSING).increment(1);
        }
        context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
    }
}
