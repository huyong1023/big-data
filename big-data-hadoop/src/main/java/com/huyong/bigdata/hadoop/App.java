package com.huyong.bigdata.hadoop;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = args[0];
        String OUTPUT_PATH = args[1];

        Configuration conf = new Configuration();




        // 0.0:首先删除输出路径的已有生成文件
        FileSystem fs = FileSystem.get(new URI(INPUT_PATH), conf);
        Path outPath = new Path(OUTPUT_PATH);
        try {
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Job job = new Job(conf, "WordCount");
       // job.setJarByClass(MyWordCountJob.class);

        // 1.0:指定输入目录
        //FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        // 1.1:指定对输入数据进行格式化处理的类（可以省略）
       // job.setInputFormatClass(TextInputFormat.class);
        // 1.2:指定自定义的Mapper类
        //job.setMapperClass(MyMapper.class);
        // 1.3:指定map输出的<K,V>类型（如果<k3,v3>的类型与<k2,v2>的类型一致则可以省略）
        //job.setMapOutputKeyClass(Display.Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 1.4:分区（可以省略）
        job.setPartitionerClass(HashPartitioner.class);
        // 1.5:设置要运行的Reducer的数量（可以省略）
        job.setNumReduceTasks(1);
        // 1.6:指定自定义的Reducer类
        //job.setReducerClass(MyReducer.class);
        // 1.7:指定reduce输出的<K,V>类型
        //job.setOutputKeyClass(Display.Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 1.8:指定输出目录
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        // 1.9:指定对输出数据进行格式化处理的类（可以省略）
        job.setOutputFormatClass(TextOutputFormat.class);
        // 2.0:提交作业
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("Success");
            System.exit(0);
        } else {
            System.out.println("Failed");
            System.exit(1);
        }
    }
}
