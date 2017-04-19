package com.guopeng.bigdata.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by guopeng on 17-4-19.
 */
public class SecondaryPartitioner extends Partitioner<SecondaryKey, IntWritable> {

    @Override
    public int getPartition(SecondaryKey key, IntWritable value, int numPartitions) {
        return (key.getFirstKey().hashCode()) % numPartitions;
    }
}