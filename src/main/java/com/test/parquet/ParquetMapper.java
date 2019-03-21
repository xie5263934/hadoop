package com.test.parquet;

import com.test.avro.ParserUtil;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ParquetMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
    private GenericRecord record = new GenericData.Record(ParserUtil.getAvroParquetSchema());

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        record.put("offset", key.get());
        record.put("line", value.toString());
        context.write(null, record);
    }
}
