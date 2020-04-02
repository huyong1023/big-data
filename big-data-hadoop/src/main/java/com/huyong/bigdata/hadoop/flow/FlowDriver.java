package com.huyong.bigdata.hadoop.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yonghu on 2020/1/15.
 */
public class FlowDriver {
    public static void main(String[]  args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取配置信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);


        //2 获取jar的存储路径
        job.setJarByClass(FlowDriver.class);


        //3 关联map和reduce的class类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4 设置map解冻输出的key和value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5 设置最后输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //6 设置分区
        job.setPartitionerClass(FlowPartitioner.class);
        // 设置reducenum个数
        job.setNumReduceTasks(5);


        //7关联combiner
        //job.setCombinerClass(FlowCombiner.class);


        String str = "/Users/yonghu/Downloads/gitlab/big-data/big-data-hadoop/data";
        String str2 = "/Users/yonghu/Downloads/gitlab/big-data/big-data-hadoop/output2";


        //7 设置输入数据的路径和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(str));
        FileOutputFormat.setOutputPath(job, new Path(str2));


        //7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);

    }
}
