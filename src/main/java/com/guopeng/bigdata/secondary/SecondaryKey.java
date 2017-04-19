package com.guopeng.bigdata.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by guopeng on 17-4-19.
 */
public class SecondaryKey implements WritableComparable<SecondaryKey> {
    private Text firstKey;
    private IntWritable secondKey;

    public SecondaryKey() {
        this.firstKey = new Text();
        this.secondKey = new IntWritable();
    }

    public Text getFirstKey() {
        return this.firstKey;
    }

    public void setFirstKey(Text firstKey) {
        this.firstKey = firstKey;
    }

    public IntWritable getSecondKey() {
        return this.secondKey;
    }

    public void setSecondKey(IntWritable secondKey) {
        this.secondKey = secondKey;
    }

    @Override
    public void readFields(DataInput dateInput) throws IOException {
        // TODO Auto-generated method stub
        this.firstKey.readFields(dateInput);
        this.secondKey.readFields(dateInput);
    }

    @Override
    public void write(DataOutput outPut) throws IOException {
        this.firstKey.write(outPut);
        this.secondKey.write(outPut);
    }

    /**
     * 自定义比较策略
     * 注意：该比较策略用于mapreduce的第一次默认排序，也就是发生在map阶段的sort小阶段，
     * 发生地点为环形缓冲区(可以通过io.sort.mb进行大小调整)
     */
    @Override
    public int compareTo(SecondaryKey secondaryKey) {
        int result = this.firstKey.compareTo(secondaryKey.getFirstKey());
        if (result == 0) {
            result = this.secondKey.compareTo(secondaryKey.getSecondKey());
        }
        return result;
    }
}
