package com.hadooptest5_2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartEmployeeMapper extends Mapper<LongWritable,Text,IntWritable,Employee>{
    @Override
    protected void map(LongWritable key1,Text value1,Context context) throws IOException,InterruptedException{
        //数据: 7654,MARTIAN,SALESMAN,7698,1981/9/28,1250,1400,30
        String data = value1.toString();
        String[] words = data.split(",");
        Employee e = new Employee();
        e.setEmpno(Integer.parseInt(words[0]));
        e.setEname(words[1]);
        e.setJob(words[2]);
        try{
            e.setMgr(Integer.parseInt(words[3]));
        }catch (Exception e1){
            e1.printStackTrace();
        }
        e.setHiredate(words[4]);
        e.setSal(Integer.parseInt(words[5]));
        try{
            e.setComm(Integer.parseInt(words[6]));
        }catch (Exception e1){
            e1.printStackTrace();
        }
        e.setDeptno(Integer.parseInt(words[7]));
        //取出部门号，将String转换成Int,Int准换成IntWritable对象
        //NullWritable k2 = NullWritable.get();
        //Employee v2 = e;
        //输出: k2 部门号 v2员工对象
        context.write(new IntWritable(e.getSal()),e);
    }
}
