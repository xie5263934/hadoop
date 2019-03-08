package com.test.sheldon;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
    @Override
    public int getPartition(IntPair intPair, NullWritable nullWritable, int i) {
        return Math.abs(intPair.getFirst() * 127) % i;
    }
}
