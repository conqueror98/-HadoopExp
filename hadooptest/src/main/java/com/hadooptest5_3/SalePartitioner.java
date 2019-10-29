package com.hadooptest5_3;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class SalePartitioner extends Partitioner<Text,SaleTable>{
    @Override
    public int getPartition(Text k2, SaleTable saleTable, int numPartitions) {
        //创建分区
        if (k2.toString() == "1999"){         //1号区
            return 1%numPartitions;
        }else if (k2.toString() == "2000") {  //2号区
            return 2%numPartitions;
        }else if (k2.toString() == "2001"){   //3号区
            return 3%numPartitions;
        }else{                                //0号区
            return 4%numPartitions;
        }
    }
}
