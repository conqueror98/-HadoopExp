package com.hadooptest5_3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class SaleTable implements Writable {
    private int prod_id;            //产品ID
    private int cust_id;            //客户ID
    private String time;            //日期
    private int channel_id;         //渠道ID
    private int promo_id;           //促销ID
    private int quantity_sold;      //销售数量
    private float amount_sold;      //销售总额

    @Override
    public String toString(){
        return prod_id+","+cust_id+","+time+","+channel_id+","+promo_id+","+quantity_sold+","+amount_sold;
    }

    public int getProd_id() {
        return prod_id;
    }
    public void setProd_id(int prod_id) {
        this.prod_id = prod_id;
    }

    public int getCust_id() {
        return cust_id;
    }
    public void setCust_id(int cust_id) {
        this.cust_id = cust_id;
    }

    public String getTime(){
        return time;
    }
    public void setTime(String time) {
        this.time = time;
    }

    public int getChannel_id() {
        return channel_id;
    }
    public void setChannel_id(int channel_id) {
        this.channel_id = channel_id;
    }

    public int getPromo_id() {
        return promo_id;
    }
    public void setPromo_id(int promo_id) {
        this.promo_id = promo_id;
    }

    public int getQuantity_sold() {
        return quantity_sold;
    }
    public void setQuantity_sold(int quantity_sold) {
        this.quantity_sold = quantity_sold;
    }

    public float getAmount_sold() {
        return amount_sold;
    }
    public void setAmount_sold(float amount_sold) {
        this.amount_sold = amount_sold;
    }

    @Override
    public void write(DataOutput output) throws IOException {   //序列化
        output.writeInt(this.prod_id);
        output.writeInt(this.cust_id);
        output.writeUTF(this.time);
        output.writeInt(this.channel_id);
        output.writeInt(this.promo_id);
        output.writeInt(this.quantity_sold);
        output.writeFloat(this.amount_sold);
    }

    @Override
    public void readFields(DataInput input) throws IOException {    //反序列化
        this.prod_id = input.readInt();
        this.cust_id = input.readInt();
        this.time = input.readUTF();
        this.channel_id = input.readInt();
        this.promo_id = input.readInt();
        this.quantity_sold = input.readInt();
        this.amount_sold = input.readFloat();
    }
}
