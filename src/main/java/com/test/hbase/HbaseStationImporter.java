package com.test.hbase;

import com.test.util.NcdcStationMetadata;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Map;

public class HbaseStationImporter extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        if (strings.length != 1) {
            System.err.println("Usage: HbaseStationImporter <input>");
            return -1;
        }
        HTable htable = new HTable(HBaseConfiguration.create(getConf()), "stations");
        NcdcStationMetadata metadata = new NcdcStationMetadata();
        metadata.initialize(new File(strings[0]));
        Map<String, String> stationMap = metadata.getStationIdToNameMap();
        for (Map.Entry<String, String> entry : stationMap.entrySet()) {
            Put put = new Put(Bytes.toBytes(entry.getKey()));
            put.add(NewHbaseStationQuery.INFO_COLUMNFAMILY, NewHbaseStationQuery.NAME_QUALIFIER, Bytes.toBytes(entry.getValue()));
            put.add(NewHbaseStationQuery.INFO_COLUMNFAMILY, NewHbaseStationQuery.DESCRIPTION_QUALIFIER, Bytes.toBytes("unknown"));
            put.add(NewHbaseStationQuery.INFO_COLUMNFAMILY, NewHbaseStationQuery.LOCATION_QUALIFIER, Bytes.toBytes("unknown"));
            htable.put(put);
        }
        htable.close();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HbaseStationImporter(), args);
        System.exit(exitCode);
    }
}
