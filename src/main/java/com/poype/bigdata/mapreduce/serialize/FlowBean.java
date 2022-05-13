package com.poype.bigdata.mapreduce.serialize;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

    private long upFlow; //上行流量

    private long downFlow; //下行流量

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
    }
}

/**
 * 如果自定义的bean要作为map task的输出key，则还需要实现Comparable接口，
 * 因为MapReduce框架中的Shuffle过程会根据Mapper的输出key对结果进行排序，所以要求key必须能支持排序。
 */
