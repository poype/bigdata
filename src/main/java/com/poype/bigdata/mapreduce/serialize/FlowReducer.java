package com.poype.bigdata.mapreduce.serialize;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {

    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long sumUp = 0;
        long sumDown = 0;
        long sumTotal = 0;

        for (FlowBean item : values) {
            sumUp += item.getUpFlow();      // 累加上行流量
            sumDown += item.getDownFlow();  // 累加下行流量
            sumTotal += (item.getUpFlow() + item.getDownFlow());  // 累加全部流量
        }

        String flowDesc = "sumUp: " + sumUp + " sumDown: " + sumDown + " sumTotal: " + sumTotal;
        outValue.set(flowDesc);

        context.write(key, outValue);
    }
}
