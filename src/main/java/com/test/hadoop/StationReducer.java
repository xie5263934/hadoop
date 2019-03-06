package com.test.hadoop;

import com.test.util.Parser;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class StationReducer extends Reducer<Text, Text, NullWritable, Text> {
    private MultipleOutputs outputs;
    private Parser parser = new Parser();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.outputs = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            parser.parse(value.toString());
            String bashPath = String.format("/%s/%s/part", parser.getStationId(), parser.getYear());
            outputs.write(NullWritable.get(), value, bashPath);
        }
    }
}
