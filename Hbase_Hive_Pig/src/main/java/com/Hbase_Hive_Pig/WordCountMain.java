package com.Hbase_Hive_Pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class WordCountMain {
    public static void main(String[] args) throws Exception{
        //从HBase中读取数据
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","192.168.20.130");
        //创建任务
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordCountMain.class);
        //定义一个扫描器读取要处理的数据
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("address"),Bytes.toBytes("city"));
        //指定Map，输入是表word
        TableMapReduceUtil.initTableMapperJob("number17034460218",scan,WordCountMapper.class, Text.class, IntWritable.class,job);
        //指定Reducer输出的表result
        TableMapReduceUtil.initTableReducerJob("result",WordCountReducer.class,job);
        job.waitForCompletion(true);
    }
}
