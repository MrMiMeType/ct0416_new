package com.atguigu.comprocess;

import com.atguigu.constant.Constant;
import com.atguigu.utils.HBaseFilterUtil;
import com.atguigu.utils.HBaseUtil;
import com.atguigu.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CalleeComprocess extends BaseRegionObserver{
    private Connection connection = null;
    private String flag = "1";
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        //获取表名
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        //获取连接和表
        String currentTableName = PropertiesUtil.getProperty("hbase.table.name");

        //判断环境中的表名与实际的表名是否一致
        if (currentTableName.equals(tableName)){
            return ;
        }

        //获取数据
        String oldRowKey = Bytes.toString(put.getRow());
        String[] splits = oldRowKey.split("_");

        //判断是否已经是新的数据
        if("1".equals(splits[4])){
            return ;
        }

        //获取属性值
        String caller = splits[1];
        String buildTime = splits[2];
        String callee = splits[3];
        String duration = splits[5];

        //获取partitionID
        String partitionID = HBaseUtil.getPartitionID(callee, buildTime, Integer.parseInt(PropertiesUtil.getProperty("hbase.regions")));

        //生成rowKey
        String rowKey = HBaseUtil.getRowKey(partitionID, callee, buildTime, caller, flag, duration);

        //为calllog对象赋值
        Put newPut = new Put(Bytes.toBytes(rowKey));
        newPut.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf2")),Bytes.toBytes(PropertiesUtil.getProperty("call1")),Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf2")),Bytes.toBytes(PropertiesUtil.getProperty("buildTime")),Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf2")),Bytes.toBytes(PropertiesUtil.getProperty("call2")),Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf2")),Bytes.toBytes(PropertiesUtil.getProperty("flag")),Bytes.toBytes(flag));
        newPut.addColumn(Bytes.toBytes(PropertiesUtil.getProperty("hbase.cf2")),Bytes.toBytes(PropertiesUtil.getProperty("duration")),Bytes.toBytes(duration));

        //获取连接，获取表对象
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table currentTable = connection.getTable(TableName.valueOf(currentTableName));

        //将值插入到表中
        currentTable.put(newPut);

        //关闭资源
        currentTable.close();
        connection.close();
    }
}
