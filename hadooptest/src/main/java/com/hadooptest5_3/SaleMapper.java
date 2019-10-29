package com.hadooptest5_3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SaleMapper extends Mapper<LongWritable,Text, Text, SaleTable> {
    @Override
    protected void map(LongWritable key1,Text value1,Context context) throws IOException,InterruptedException{
        //数据:147,1589,2001-10-18,3,999,1,8.07
        String data = value1.toString();
        //分词
        String[] words = data.split(",");
        //将时间切分至年份
        String[] in = words[2].split("-");
        //创建对象
        SaleTable e = new SaleTable();
        e.setProd_id(Integer.parseInt(words[0]));
        e.setCust_id(Integer.parseInt(words[1]));
        e.setTime(in[0]);
        e.setChannel_id(Integer.parseInt(words[3]));
        e.setPromo_id(Integer.parseInt(words[4]));
        e.setQuantity_sold(Integer.parseInt(words[5]));
        e.setAmount_sold(Float.parseFloat(words[6]));
        context.write(new Text(e.getTime()),e);
    }
}
