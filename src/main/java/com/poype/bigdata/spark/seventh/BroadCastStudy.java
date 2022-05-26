package com.poype.bigdata.spark.seventh;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadCastStudy {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, String> studentMap = new HashMap<>();
        studentMap.put("001", "张三");
        studentMap.put("002", "李四");
        studentMap.put("003", "王五");
        studentMap.put("004", "星辰");
        studentMap.put("005", "名博");

        // 构建广播变量，广播变量以进程为单位分发给所有的executor，而不是以task线程为单位
        // 一个executor进程中包含多个task线程，广播变量只会发送一次给进程，进程中的所有task线程就都能获取到这个变量了
        Broadcast<Map<String, String>> studentMapBroadcast = sc.broadcast(studentMap);

        List<Map<String, String>> scoreList = new ArrayList<>();
        scoreList.add(createScore("001", "语文", "88"));
        scoreList.add(createScore("002", "语文", "87"));
        scoreList.add(createScore("003", "语文", "86"));
        scoreList.add(createScore("004", "语文", "85"));
        scoreList.add(createScore("005", "语文", "84"));
        scoreList.add(createScore("001", "数学", "83"));
        scoreList.add(createScore("002", "数学", "82"));
        scoreList.add(createScore("003", "数学", "81"));
        scoreList.add(createScore("004", "数学", "91"));
        scoreList.add(createScore("005", "数学", "90"));

        JavaRDD<Map<String, String>> scoreListRdd = sc.parallelize(scoreList, 3);

        // 用学生的名字替换ID, studentMap对象会发送给所有的partition，即发送给所有的task线程
        JavaRDD<Map<String, String>> resultRdd = scoreListRdd.map(scoreObj -> {
            String id = scoreObj.get("id");
            // broadcast.value引用广播变量
            String studentName = studentMapBroadcast.value().get(id);
            scoreObj.remove("id");
            scoreObj.put("name", studentName);
            return scoreObj;
        });

        System.out.println(resultRdd.glom().collect());
    }

    private static Map<String, String> createScore(String id, String subject, String score) {
        Map<String, String> map = new HashMap<>();
        map.put("id", id);
        map.put("subject", subject);
        map.put("score", score);
        return map;
    }
}

/**
 * executor是进程
 * 一个executor进程可以处理多个分区，每个分区是由task线程处理的
 * 一个executor进程中可以包含多个task线程，进程之中数据共享
 */
