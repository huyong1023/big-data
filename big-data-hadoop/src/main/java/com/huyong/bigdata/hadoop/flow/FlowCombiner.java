package com.huyong.bigdata.hadoop.flow;

import com.mysql.jdbc.util.Base64Decoder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yonghu on 2020/1/16.
 */
public class FlowCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {



    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value : values){
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}
