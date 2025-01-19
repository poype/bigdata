package com.poype;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Each flink program consists of the same basic parts:
 * 1. Obtain an execution environment
 * 2. Load/create the initial data
 * 3. Specify transformations on this data
 * 4. Specify where to put the results of your computations
 * 5. Trigger the program execution
 */
public class StudyDataStream {

    public static void main(String[] args) throws Exception {

        // The StreamExecutionEnvironment is the basis for all Flink programs.
        // 可以通过StreamExecutionEnvironment中三个static method 获取一个 StreamExecutionEnvironment 对象
        // getExecutionEnvironment, createLocalEnvironment, createRemoteEnvironment
        // Typically, you only need to use getExecutionEnvironment(), since this will do the right thing depending on the context
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9999);

        // flagMap方法参数类型是FlatMapFunction
        DataStream<Tuple2<String, Integer>> wordStream = socketSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        DataStream<Tuple2<String, Integer>> resultStream =
                wordStream.keyBy(wordTuple -> wordTuple.f0)
                        .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                        .sum(1);

        // Once you have a DataStream containing your final results, you can write it to an outside system by creating a sink.
        // writeAsText(String path); 和 print(); 都可以创建一个sink
        resultStream.print();


        // To run the example program, start the input stream with netcat first from a terminal:
        // nc -lk 9999
        env.execute("Window WordCount");
    }
}









