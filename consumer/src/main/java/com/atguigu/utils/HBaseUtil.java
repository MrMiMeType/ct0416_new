package com.atguigu.utils;

import com.atguigu.constant.Constant;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

public class HBaseUtil {
    //创建命名空间
    public static void createNamespace(String namespace) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);

        //创建admin对象
        Admin admin = connection.getAdmin();

        //创建命名空间描述器对象
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

        //创建命名空间
        admin.createNamespace(namespaceDescriptor);

        //关闭资源
        admin.close();
        connection.close();
    }

    //判断表是否存在
    public static boolean isTableExist(String table) throws IOException {

        //获取连接
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Admin admin = connection.getAdmin();

        //获取表
        boolean exists = admin.tableExists(TableName.valueOf(table));

        //关闭资源

        admin.close();
        connection.close();

        return exists;
    }

    //创建表
    public static void createTable(String table, String... cfs) throws IOException {

        //创建连接&admin
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Admin admin = connection.getAdmin();

        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(table));

        //创建列描述器
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        int regions = Integer.parseInt(PropertiesUtil.getProperty("hbase.regions"));

        //创建表
        admin.createTable(hTableDescriptor, getSplits(regions));

        //关闭资源
        admin.close();
        connection.close();
    }

    //生成分区键
    private static byte[][] getSplits(int regions) {

        //声明二维数组
        byte[][] splits = new byte[regions][];

        //格式化分区
        DecimalFormat df = new DecimalFormat("00");

        //赋值
        for (int i = 0; i < regions; i++) {
            splits[i] = Bytes.toBytes(df.format(i) + "|");
        }

        //返回
        return splits;
    }

    //生成分区号
    public static String getPartitionID(String call1, String buildTime, int regions) {

        //取手机号后4位
        String last4Num = call1.substring(7);

        //取年月
        String yearMonth = buildTime.replace("-", "").substring(0, 6);

        int hashCode = (Integer.parseInt(last4Num) ^ Integer.parseInt(yearMonth)) % regions;

        return new DecimalFormat("00").format(hashCode);

    }

    //rowkey设计
    public static String getRowKey(String partitionID, String caller, String buildTime, String callee, String flag, String duration) {

        return partitionID + "_" + caller + "_" + buildTime + "_" + callee + "_" + flag + "_" + duration;

    }

}
