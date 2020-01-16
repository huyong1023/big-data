package com.huyong.bigdata.hadoop.flow;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Created by yonghu on 2020/1/15.
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean bean = new FlowBean();
    Text k = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        
        String phoneNum = fields[1];
        
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        bean.set(upFlow, downFlow);
        k.set(phoneNum);

        context.write(k, bean);
    }


}
