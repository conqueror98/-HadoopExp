package com.hadooptest5_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
public class MyEmployeePartitioner extends Partitioner<IntWritable,Employee>{
    @Override
    public int getPartition(IntWritable k2, Employee employee, int numPartitions) {
        //创建分区
        if(employee.getSal()<1500){     //1号区
            return 1%numPartitions;
        }else if (employee.getSal()>=1500 && employee.getSal() < 3000 ){    //2号区
            return 2%numPartitions;
        }else {     //3号区
            return 3%numPartitions;
        }
        /*if (employee.getDeptno() == 10) {
            return 1%numPartitions;
        }else if (employee.getDeptno() == 20){
            return 2%numPartitions;
        }else {
            return 3%numPartitions;
        }
         */
    }
}
