package com.poype.bigdata.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context)
            throws IOException, InterruptedException {

        // 通过iterator标记起始位置
        ReduceContext.ValueIterator<TableBean> iterator =
                (ReduceContext.ValueIterator<TableBean>) values.iterator();
        iterator.mark();

        TableBean pdBean = new TableBean();

        // 找到商品表的那个记录，并将结果存到pdBean中
        for (TableBean value : values) {
            if ("pd".equals(value.getFlag())) {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                // 已经找到了商品记录，后面的记录无需遍历了
                break;
            }
        }

        // 让iterator重新回到起点，要重新遍历
        // 如果没有reset这一步，则iterator不会重头开始，即下面的for循环都不会执行
        iterator.reset();
        for (TableBean value : context.getValues()) {
            if ("order".equals(value.getFlag())) {
                value.setPname(pdBean.getPname());
                context.write(value, NullWritable.get());
            }
        }
    }
}
