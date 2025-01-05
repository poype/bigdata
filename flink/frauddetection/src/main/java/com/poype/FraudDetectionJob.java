package com.poype;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * The FraudDetectionJob class defines the data flow of the application
 * and the FraudDetector class defines the business logic of the function that detects fraudulent transactions.
 */

public class FraudDetectionJob {

    /**
     * 定义了整个Application的 data flow，具体的业务逻辑在FraudDetector中
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sources ingest data from external systems, such as Apache Kafka, into Flink Jobs.
        // 这里获得了一个装满Transaction对象的DataStream，从该DataStream中可以源源不断的获取Transaction对象。
        DataStream<Transaction> transactions = env.addSource(new TransactionSource()).name("transactions");

        // 给每个operator都指定了name，这些name会在flink webUI的Job管理页面中显示

        // transactions DataStream中包含了所有用户的Transaction对象，会有多个fraud detection tasks对其进行处理。
        // 要确保同一个账号的所有Transaction对象都发给同一个fraud detection task进行处理。
        // keyBy根据accountId参数对transactions DataStream进行分区，这样具有相同accountId的所有transaction都将由一个fraud detection task处理
        // process方法指定了一个operator（FraudDetector对象），operator会针对分区后的每一个transaction执行相应业务逻辑。
        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        // A sink writes a DataStream to an external system; such as Apache Kafka, Cassandra.
        // 这里只是简单的打印日志
        alerts.addSink(new AlertSink()).name("send-alerts");

        // 指定Job的名字
        env.execute("Fraud Detection");
    }
}
