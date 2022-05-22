package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> employee = new ArrayList<>();
        List<Tuple2<String, String>> department = new ArrayList<>();

        // 部门ID和员工姓名
        employee.add(new Tuple2<>("1001", "zhangsan"));
        employee.add(new Tuple2<>("1002", "lisi"));
        employee.add(new Tuple2<>("1002", "wangwu"));
        employee.add(new Tuple2<>("1003", "Poype"));

        // 部门ID和部门姓名
        department.add(new Tuple2<>("1001", "销售部"));
        department.add(new Tuple2<>("1002", "科技部"));

        JavaPairRDD<String, String> employeeRdd = sc.parallelizePairs(employee);
        JavaPairRDD<String, String> departmentRdd = sc.parallelizePairs(department);
        // join方法会自动用key关联
        JavaPairRDD<String, Tuple2<String, String>> resultRdd = employeeRdd.join(departmentRdd);

        // [(1001,(zhangsan,销售部)), (1002,(lisi,科技部)), (1002,(wangwu,科技部))]
        System.out.println(resultRdd.collect());

        // left join就要用Optional类型了
        JavaPairRDD<String, Tuple2<String, Optional<String>>> leftJoinResultRdd =
                employeeRdd.leftOuterJoin(departmentRdd);
        // [(1001,(zhangsan,Optional[销售部])), (1002,(lisi,Optional[科技部])), (1002,(wangwu,Optional[科技部])), (1003,(Poype,Optional.empty))]
        System.out.println(leftJoinResultRdd.collect());
    }
}

// join算子只能用于pairRDD

