package com.poype;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.IOException;

/**
 * 业务需求: 监控transaction数据流，当满足特定条件时就认为交易有安全风险，向下游输出一个Alert对象。
 * 1.the fraud detector should output an alert for account that makes a small transaction
 *   immediately followed by a large one. Where small is anything less than $1.00 and large is more than $500.
 * 2.small transaction 和 large transaction 的时间间隔不超过1分钟
 *
 * To do this, the fraud detector must remember information across events;
 * Remembering information across events requires state.
 */

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {
    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    // 相比于普通的Map类型变量，Flink提供的State类型变量具有fault-tolerant特性, fault tolerance is managed automatically by Flink under the hood
    // The most basic type of state in Flink is ValueState, it is a form of keyed state.
    // meaning it is only available in operators that are applied in a keyed context; any operator immediately following DataStream#keyBy.
    // 因此，此处的 State 对象具备自动与 accountId 关联的特性，因为keyBy是根据accountId对DataStream进行分区的。
    // 其本质上等同于一个 Map 类型的对象，其中的键即为 accountId 的取值，只不过该键值在表现形式上被隐含处理，不直观呈现而已。
    private transient ValueState<Boolean> flagState;

    private transient ValueState<Long> timerState;

    // The state should be registered before the function starts processing data. The right hook for this is the open() method.
    @Override
    public void open(OpenContext openContext) throws Exception {
        // ValueState is created using a ValueStateDescriptor which contains metadata about how Flink should manage the variable.
        ValueStateDescriptor<Boolean> flagDescriptor =
                new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        // 1. Whenever the flag is set to true, also set a timer for 1 minute in the future.
        // 2. When the timer fires, reset the flag by clearing its state. 这样flag的生命周期最多只有1分钟
        // 3. If the flag is cleared the timer should be canceled.
        // 4. To cancel a timer, you have to remember what time it is set for, and remembering implies state.
        //    所以这需要定义这个timerState用于记录设置timer的时间戳。
        ValueStateDescriptor<Long> timerDescriptor =
                new ValueStateDescriptor<>("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);

        // ValueState provides three methods for interacting with its contents:
        // 1. update sets the state,
        // 2. value gets the current value,
        // 3. clear deletes its contents.

        // ValueState对象有点类似ThreadLocal，
        // 从ThreadLocal中读取和写入的值始终是跟某一个thread关联的
        // 从ValueState中读取和写入的值始终是跟某一个key关联的，这里的key就是accountID。
    }

    // the method processElement is called for every transaction event.
    @Override
    public void processElement(Transaction transaction,
                               Context context,
                               Collector<Alert> collector) throws IOException {

        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();

        // Check if the flag is set
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                // Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }
            // Clean up our state
            cleanUp(context);
        }

        // 当收到一笔小额支付时，将flag设置成true，并注册一个timer，timer在一分钟后执行，目的是清除flag。
        // 因为需求是发生在一分钟之内小额支付和大额支付才被认为有fraud风险。
        if (transaction.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);

            // The timer service can be used to query the current time, register timers, and delete timers.
            // context.timerService().currentProcessingTime()获取的不是传统意义上的系统当前时间，所以这里不能用new Date()代替。
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }

        // For flagState, this job only makes use of unset (null) and true to check whether the flag is set or not.

        // 这里所有的valueState 和 timer 都是在一个keyed context内
    }

    // When a timer fires, it calls onTimer method.
    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<Long, Transaction, Alert>.OnTimerContext ctx,
                        Collector<Alert> out) throws Exception {
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context context) throws IOException {
        Long timer = timerState.value();

        // 要删除一个Timer，必须清楚知道它所设定的时间，所以才需要定义一个timerState用于记录时间。
        // Timers can internally be scoped to keys and/ or windows. When you delete a timer, it is removed from the current keyed context.
        // timer的作用域是在keyed context，所以即使有两个trigger time相同的timer，只要这两个timer是针对不同的key设置的，就不会被误删除。
        context.timerService().deleteProcessingTimeTimer(timer);

        flagState.clear();
        timerState.clear();
    }
}
