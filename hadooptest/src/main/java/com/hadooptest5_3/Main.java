package com.hadooptest5_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Main {
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        //创建一个Job
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Main.class);               //main方法所在的class
        //指定job的mapper和输出的类型<k2,v2>,单独开一个线程运行
        job.setMapperClass(SaleMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SaleTable.class);
        //指定分区规则
        job.setPartitionerClass(SalePartitioner.class);
        job.setNumReduceTasks(4);
        //指定job的reducer1和输出的类型<k4,v4>,单独开一个线程运行
        job.setReducerClass(SaleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //指定job的输入和输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //执行
        job.waitForCompletion(true);
        System.out.println( "Hello World!" );
    }
}
