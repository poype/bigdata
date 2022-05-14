package com.poype.bigdata.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // 1 累加求和
        int sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }

        // 2 输出
        outValue.set(sum);
        context.write(key, outValue);
    }
}
