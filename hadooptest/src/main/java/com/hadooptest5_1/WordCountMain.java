package com.hadooptest5_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WordCountMain {
    public static void main( String[] args ) throws Exception {
        BasicConfigurator.configure();
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCountMain.class);
        //指定job的mapper    k2 v2
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定job的Combiner  k3,v3
        job.setCombinerClass(MyCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job的reducer   k4 v4
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(WordCountReducer.class);
        job.setOutputValueClass(IntWritable.class);
        //指定job的输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //执行任务
        job.waitForCompletion(true);
    }
}
