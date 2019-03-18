package com.test.avro;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroTest2 {
    public static void main(String [] args) throws IOException {
        StringPair stringPair = new StringPair();
        stringPair.setLeft("L");
        stringPair.setRight("R");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<StringPair> writer = new SpecificDatumWriter<>(StringPair.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out,null);
        writer.write(stringPair,encoder);
        encoder.flush();
        out.close();
        DatumReader<StringPair> reader = new SpecificDatumReader<>(StringPair.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
        StringPair result = reader.read(null,decoder);
        System.out.println(result.getLeft());
        System.out.println(result.getRight());
    }
}
