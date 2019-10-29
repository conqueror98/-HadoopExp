package com.hadooptest5_4;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class DataPartitioner extends Partitioner<Text,Data> {
    @Override
    public int getPartition(Text text, Data data, int numPartitions) {
        //分区
        if (data.getRank_in_research() == 2 && data.getClick_in_research() == 1){
            //1号区
            return 1/numPartitions;
        }
        else {  //2号区
           return 2/numPartitions;
        }
    }
}
