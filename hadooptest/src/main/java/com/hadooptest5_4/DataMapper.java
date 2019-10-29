package com.hadooptest5_4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<LongWritable, Text, Text, Data> {
    @Override
    protected void map(LongWritable key1,Text value1,Context context) throws IOException,InterruptedException {
        //数据
        String data = value1.toString();
        //按空格分词
        //数据 00:00:00	2982199073774412	[360安全卫士]	8 3	download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html
        String[] words = data.split("\t");
        //创建Data类的对象
        Data da = new Data();
        da.setTime(words[0]);
        da.setNumber(words[1]);
        da.setName(words[2]);
        String[] str = words[3].split(" ");
        da.setRank_in_research(Integer.parseInt(str[0]));
        da.setClick_in_research(Integer.parseInt(str[1]));
        da.setUrl(words[4]);
        context.write(new Text(da.getNumber()),da);
    }
}
