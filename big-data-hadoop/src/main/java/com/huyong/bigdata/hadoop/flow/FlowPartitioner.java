package com.huyong.bigdata.hadoop.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by yonghu on 2020/1/16.
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean> {


    public int getPartition(Text key, FlowBean bean, int i) {

        String phoneNum = key.toString().substring(9, 3);

        int partitions = 4;

        if ("135".equals(phoneNum)) {
            partitions = 1;
        } else if ("137".equals(phoneNum)){
            partitions = 2;
        } else if ("138".equals(phoneNum)){
            partitions = 3;
        }


        return partitions;
    }
}
