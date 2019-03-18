package com.test.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class AvroTest3 {
    public static void main(String[] args) throws IOException {
        AvroTest3 test3 = new AvroTest3();
        test3.writerFile();
        test3.readFile();
    }

    public void writerFile() throws IOException {
        GenericRecord datum = new GenericData.Record(getSchema());
        datum.put("left", "LEFT-ONE");
        datum.put("right", "RIGHT-ONE");
        File file = new File("data.avro");
        DatumWriter writer = new GenericDatumWriter(getSchema());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(getSchema(), file);
        dataFileWriter.append(datum);
        dataFileWriter.close();
    }

    public Schema getSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new FileInputStream("target/classes/StringPair.avsc"));
        return schema;
    }

    public void readFile() throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(getSchema());
        File file = new File("data.avro");
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, reader);
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next();
            System.out.println(record.get("left").toString());
            System.out.println(record.get("right").toString());
        }
    }


}
