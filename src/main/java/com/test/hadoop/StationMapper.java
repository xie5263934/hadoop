package com.test.hadoop;

import com.test.util.Parser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StationMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Parser parser = new Parser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value.toString());
        context.write(new Text(parser.getStationId()), value);
    }

}
