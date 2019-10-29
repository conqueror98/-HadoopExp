package com.hadooptest5_2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PartEmployeeReducer extends Reducer<IntWritable,Employee,IntWritable,Employee>{
    @Override
    protected void reduce(IntWritable k3,Iterable<Employee> v3,Context context) throws IOException,InterruptedException{
        for(Employee v : v3){
            context.write(k3,v);    //将分区的数据写入对应的文件中
        }
    }
}