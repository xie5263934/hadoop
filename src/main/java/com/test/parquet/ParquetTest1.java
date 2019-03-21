package com.test.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;

import java.io.FileInputStream;
import java.io.IOException;

public class ParquetTest1 {
    public static void main(String[] args) throws IOException {
        ParquetTest1 test1 = new ParquetTest1();
        test1.readProject();
    }

    private void writeData() throws IOException {
        Schema schema = new Schema.Parser().parse(new FileInputStream("target/classes/StringPair.avsc"));
        GenericRecord record = new GenericData.Record(schema);
        record.put("left", "LEFT");
        record.put("right", "RIGHT");
        Path path = new Path("data.parquet");
        AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(path, schema);
        writer.write(record);
        writer.close();
    }

    private void readData() throws IOException {
        Path path = new Path("data.parquet");
        AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(path);
        GenericRecord record = reader.read();
        if (record != null) {
            System.out.println(record.get("left").toString());
            System.out.println(record.get("right").toString());
        }
    }

    private void readProject() throws IOException{
        Schema schema = new Schema.Parser().parse(new FileInputStream("target/classes/StringProject.avsc"));
        Configuration configuration = new Configuration();
        AvroReadSupport.setRequestedProjection(configuration,schema);
        Path path = new Path("data.parquet");
        AvroParquetReader<GenericRecord> reader = new AvroParquetReader<>(configuration,path);
        GenericRecord record = reader.read();
        System.out.println(record.get("left")==null?0:1);
        System.out.println(record.get("right").toString());
    }
}
