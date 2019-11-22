package com.Hbase_Hive_Pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Hbase {
    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.20.130,192.168.20.131,192.168.20.132");
        configuration.set("hbase.rootdir","hdfs://192.168.20.130:8020");
        configuration.set("hbase.cluster.distributed","true");
    }
    public static void main( String[] args ) throws Exception {
        //createTable();
        //insert();
        //get();
        scan();
        //delete();
        //delete_some();
    }

    public static void createTable() throws Exception {
        /*
            参照HBase/conf/hbase-site.xml的配置来修改
            在windows下的C:\WINDOWS\System32\drivers\etc\hosts加入ip对应的host
            192.168.20.130  node1
            192.168.20.131  node2
            192.168.20.132  node3
         */
        //创建到HBase的连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //Admin admin = connection.getAdmin();
        HBaseAdmin client = new HBaseAdmin(connection);
        //Table的描述，把列族加入描述当中去
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("student"));
        //Table的列族
        String[] family = new String[]{"info","address","grade"};
        for (int i = 0;i < family.length;i++){
            htd.addFamily(new HColumnDescriptor(family[i]));
        }
        if (client.tableExists(TableName.valueOf("student"))){
            //判断已存在
            System.out.println("创建表失败,表已经存在了!");
            System.exit(0);
        }else {     //判断不存在
            client.createTable(htd);
            System.out.println("建表成功!");
        }
    }

    public static void scan() throws Exception{
        //HTable hTable = new HTable(configuration,"emp17034460218");
        //指定表的客户端
        HTable table = new HTable(configuration,"number17034460218");
        //创建一个扫描器Scan,相当于select * from emp0218
        Scan scan = new Scan();
        //scan.setFilter(filter);过滤器
        //执行查询,返回ResultScanner的Oracle中的游标
        ResultScanner resultScanner = table.getScanner(scan);
        System.out.println("获取所有的数据");
        for (Result result : resultScanner){
            String name = Bytes.toString(result.getRow());
            String city = Bytes.toString(result.getValue(Bytes.toBytes("address"),Bytes.toBytes("city")));
            String country = Bytes.toString(result.getValue(Bytes.toBytes("address"),Bytes.toBytes("country")));
            System.out.println( "姓名:"+ name + "\r\n" + "国家" + country + "\r\n" + "城市:" + city);
            System.out.println(result + "\r\n");
        }
        table.close();
    }

    public static void insert() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","192.168.20.130");
        //指定表的客户端
        HTable table = new HTable(configuration,"student");
        //构造一条数据
        Put put = new Put(Bytes.toBytes("DaHai"));
        put.addColumn(Bytes.toBytes("info"),        //列族的名字
                Bytes.toBytes("name"),              //列的名字
                Bytes.toBytes("aixiaoting"));              //值
        table.put(put);
        table.close();
        System.out.println("插入数据成功！" + put);
    }

    public static void get() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","192.168.20.130");
        //指定表的客户端
        HTable table = new HTable(configuration,"number17034460218");
        //通过Get查询
        Get get = new Get(Bytes.toBytes("number17034460218"));
        //执行查询
        Result result = table.get(get);
        //输出
        String name = Bytes.toString(result.getValue(Bytes.toBytes("Sariel"),Bytes.toBytes("info")));
        System.out.println("获取一条数据");
        System.out.println(name);
        table.close();
    }

    //删除表中的一条数据
    public static void delete_some() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","192.168.20.130");
        //创建一个Hbase的客户端
        HBaseAdmin client = new HBaseAdmin(configuration);
        client.deleteColumn("student", "info");
        client.close();
        System.out.println("删除一条数据!");
    }

    //删除整张表格
    public static void delete() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","192.168.20.130");
        //创建一个Hbase的客户端
        HBaseAdmin client = new HBaseAdmin(configuration);
        client.disableTable("jjj");
        client.deleteTable("jjj");
        client.close();
    }
}

