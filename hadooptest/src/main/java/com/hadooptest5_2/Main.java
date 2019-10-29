package com.hadooptest5_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
        //指定job的mapper和输出的类型<k2,v2>
        job.setMapperClass(PartEmployeeMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);           //部门号
        job.setMapOutputValueClass(Employee.class);             //员工
        //指定分区规则
        job.setPartitionerClass(MyEmployeePartitioner.class);
        job.setNumReduceTasks(3);
        //指定job的reducer和输出的类型<k4,v4>
        job.setReducerClass(PartEmployeeReducer.class);
        job.setOutputKeyClass(IntWritable.class);              //部门号
        job.setOutputValueClass(Employee.class);                    //员工
        //指定job的输入和输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //执行
        job.waitForCompletion(true);
        System.out.println( "Hello World!" );
    }
}
