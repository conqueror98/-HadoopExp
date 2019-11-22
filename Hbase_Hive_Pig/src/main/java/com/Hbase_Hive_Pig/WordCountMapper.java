package com.Hbase_Hive_Pig;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WordCountMapper extends TableMapper<Text,IntWritable>{
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,InterruptedException{
        String data = Bytes.toString(value.getValue(Bytes.toBytes("address"),Bytes.toBytes("city")));
        String[] words = data.split(" ");
        for (String w : words){
            context.write(new Text(w),new IntWritable(1));
        }
    }

}
