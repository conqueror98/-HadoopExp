package com.hadooptest5_4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class DataReducer extends Reducer<Text,Data,Text,Text> {
    @Override
    protected void reduce(Text k3,Iterable<Data> v3,Context context) throws IOException, InterruptedException {
        for (Data d : v3){
            String str = "URL 排名:" + d.getRank_in_research() + "    用户点击顺序:" + d.getClick_in_research();
            String str1 = d.getName() + "   " + d.getUrl();
            context.write(new Text(str),new Text(str1));
        }
    }
}
