package com.hadooptest5_3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaleReducer extends Reducer<Text, SaleTable,Text,Text> {
    @Override
    protected void reduce(Text k3, Iterable<SaleTable> v3, Context context) throws IOException,InterruptedException{
        int total1 = 0;
        int total2 = 0;
        for(SaleTable v :v3){
            total1 += v.getQuantity_sold();
            total2 += v.getAmount_sold();
        }
        String str = "Quantity_sold(销售个数):" + total1 + "  Amount_sold(销售金额):" + total2;
        context.write(k3,new Text(str));
    }
}
