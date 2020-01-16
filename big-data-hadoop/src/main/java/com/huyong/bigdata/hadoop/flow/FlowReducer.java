package com.huyong.bigdata.hadoop.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Created by yonghu on 2020/1/15.
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sum_upFlow = 0;
        long sum_downFlow = 0;

        for (FlowBean bean : values) {
            sum_downFlow += bean.getDownFlow();
            sum_upFlow += bean.getUpFlow();
        }


        context.write(key, new FlowBean(sum_upFlow, sum_downFlow));
    }

}
