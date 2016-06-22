package com.ryxc.ods.mms;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2015/12/27.
 */
public class MmsRecord implements WritableComparable<Object> {
    public String flow;
    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        flow = in.readUTF();
    }
}
