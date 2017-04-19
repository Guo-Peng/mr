package com.guopeng.bigdata.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by guopeng on 17-4-19.
 */
public class SecondaryGroup extends WritableComparator {
    public SecondaryGroup() {
        super(SecondaryKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        SecondaryKey ck1 = (SecondaryKey) a;
        SecondaryKey ck2 = (SecondaryKey) b;
        return ck1.getFirstKey().compareTo(ck2.getFirstKey());
    }
}