package com.hadooptest5_1;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MyCombiner extends Reducer<Text,IntWritable,Text,IntWritable>  {
    @Override
    protected void reduce(Text k2, Iterable<IntWritable> v2,Context context) throws IOException,InterruptedException{
        int total = 0;
        for(IntWritable v:v2){
            total += v.get();
        }
        //输出 k4,v4
        context.write(k2,new IntWritable(total));
    }
}
