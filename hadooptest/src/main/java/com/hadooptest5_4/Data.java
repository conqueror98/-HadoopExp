package com.hadooptest5_4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Data implements Writable {
    private String time;
    private String number;
    private String name;
    private int rank_in_research;       //URL地址在搜索结果中的排名
    private int click_in_research;      //用户在搜索的URL地址中点击的顺序
    private String url;

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getNumber(){
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getName(){
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRank_in_research() {
        return rank_in_research;
    }

    public void setRank_in_research(int rank_in_research) {
        this.rank_in_research = rank_in_research;
    }

    public int getClick_in_research(){
        return click_in_research;
    }

    public void setClick_in_research(int click_in_research) {
        this.click_in_research = click_in_research;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public void write(DataOutput output) throws IOException {   //序列化
        output.writeUTF(this.time);
        output.writeUTF(this.number);
        output.writeUTF(this.name);
        output.writeInt(this.rank_in_research);
        output.writeInt(this.click_in_research);
        output.writeUTF(this.url);
    }

    @Override
    public void readFields(DataInput input) throws IOException {   //反序列化
        this.time = input.readUTF();
        this.number = input.readUTF();
        this.name = input.readUTF();
        this.rank_in_research = input.readInt();
        this.click_in_research = input.readInt();
        this.url = input.readUTF();
    }
}
