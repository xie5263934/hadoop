package com.test.avro;

import org.apache.avro.Schema;

public class ParserUtil {
    private static Schema schema;

    public static Schema getSchema() {
        if (schema == null) {
            schema = new Schema.Parser().parse("{\n" +
                    "\"namespace\": \"com.test.avro\",\n" +
                    "\"type\":\"record\",\n" +
                    "\"name\":\"weatherRecord\",\n" +
                    "\"doc\":\"A weather reading.\",\n" +
                    "\"fields\":[\n" +
                    "{\"name\":\"year\",\"type\":\"int\"},\n" +
                    "{\"name\":\"temperature\",\"type\":\"int\"},\n" +
                    "{\"name\":\"stationId\",\"type\":\"string\"}\n" +
                    "]\n" +
                    "}");
        }
        return schema;
    }
}
