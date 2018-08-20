package com.atguigu.hbaseconsumer;

import com.atguigu.constant.Constant;
import com.atguigu.utils.HBaseUtil;
import com.atguigu.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDAO {
    //属性私有化
    private String namespace = null;
    private String tableName = null;
    private int regions = 1;
    private List<Put> puts = null;
    private Connection connection = null;
    private Table table = null;
    private String flag = null;

    //初始化属性
    public HBaseDAO() throws IOException {
        namespace = PropertiesUtil.getProperty("hbase.namespace");
        tableName = PropertiesUtil.getProperty("hbase.table.name");
        regions = Integer.parseInt(PropertiesUtil.getProperty("hbase.regions"));
        puts = new ArrayList<>();
        connection = ConnectionFactory.createConnection(Constant.CONF);
        table = connection.getTable(TableName.valueOf(tableName));
        flag = "0";
        //创建命名空间和表
        try {
            HBaseUtil.createNamespace(namespace);
        } catch (Exception e) {
            System.out.println("命名空间已存在！");
        }
        if(!HBaseUtil.isTableExist(tableName)){
            HBaseUtil.createTable(tableName, PropertiesUtil.getProperty("hbase.cf1"), PropertiesUtil.getProperty("hbase.cf2"));
        }
    }

    //批量插入数据
    public void puts(String line) throws IOException {

        //将数据进行切割
        String[] splits = line.split(",");

        //判断数据是否合法
        if (splits.length != 4){
            return;
        }

        //封装属性
        String caller = splits[0];
        String callee = splits[1];
        String buildTime = splits[2];
        String duration = splits[3];

        //获取分区号
        String partitionID = HBaseUtil.getPartitionID(caller,buildTime,regions);

        //获取rowKey
        String rowKey = HBaseUtil.getRowKey(partitionID, caller, buildTime, callee,flag, duration);

        //封装对象
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf1")),Bytes.toBytes("call1"),Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf1")),Bytes.toBytes("call2"),Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf1")),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf1")),Bytes.toBytes("flag"),Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf1")),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        //将对象封装到集合中
        puts.add(put);

        //根据集合的大小写到hbase,并清空集合
        if(puts.size() >= 20){
            table.put(puts);
            puts.clear();
        }

    }

    //设置一定时间间隔put，解决很长时间集合中元素个数都未满20条的情况,关键是怎样获取上一次提交的时间？这个需要开启另一个线程来监控，zookeeper？
    public void timePuts() throws IOException {
        table.put(puts);
    }

    //关闭资源
    public void close() throws IOException {
        table.put(puts);
        table.close();
        connection.close();
    }
}
